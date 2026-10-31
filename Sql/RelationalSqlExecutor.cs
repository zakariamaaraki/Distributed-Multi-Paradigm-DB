using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using LsmWriteDb.Storage;
using LsmWriteDb.Transactions;

namespace LsmWriteDb.Sql;

internal sealed class RelationalSqlExecutor
{
    private readonly DatabaseEngine _database;
    private readonly TransactionManager _transactions;

    public RelationalSqlExecutor(DatabaseEngine database, TransactionManager transactions)
    {
        _database = database;
        _transactions = transactions;
    }

    public async Task<SqlExecutionResult?> TryExecuteAsync(string sql, Guid? transactionId)
    {
        var keyword = sql.TrimStart().Split(' ', StringSplitOptions.RemoveEmptyEntries)[0].ToUpperInvariant();
        if (keyword is not ("INSERT" or "SELECT" or "UPDATE" or "DELETE"))
            return null;

        var tablePattern = keyword switch
        {
            "INSERT" => @"\bINTO\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)",
            "SELECT" => @"\bFROM\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)",
            "UPDATE" => @"^\s*UPDATE\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)",
            "DELETE" => @"\bFROM\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)",
            _ => string.Empty
        };
        var tableMatch = Regex.Match(sql, tablePattern, RegexOptions.IgnoreCase);
        if (!tableMatch.Success)
            return null;
        var table = tableMatch.Groups["table"].Value.ToLowerInvariant();
        var schema = await _database.GetRelationalSchemaAsync(table);
        if (schema is null)
        {
            if (keyword == "SELECT"
                && await _database.GetVectorSchemaAsync(table) is not null
                && Regex.IsMatch(sql, @"\bORDER\s+BY\b", RegexOptions.IgnoreCase))
                return await VectorSelectAsync(sql, table);
            return null;
        }
        if (keyword == "INSERT" && sql.Contains("{", StringComparison.Ordinal))
            return null;
        if (keyword is "SELECT" or "UPDATE" or "DELETE" && (Regex.IsMatch(sql, @"\bkey\b", RegexOptions.IgnoreCase) || Regex.IsMatch(sql, @"\bvalue\b", RegexOptions.IgnoreCase)))
            return null;
        return keyword switch
        {
            "INSERT" => await InsertAsync(sql, table, schema, transactionId),
            "SELECT" => await SelectAsync(sql, table, schema),
            "UPDATE" => await UpdateAsync(sql, table, schema, transactionId),
            "DELETE" => await DeleteAsync(sql, table, schema, transactionId),
            _ => null
        };
    }

    private async Task<SqlExecutionResult> VectorSelectAsync(string sql, string table)
    {
        var match = Regex.Match(sql,
            @"^\s*SELECT\s+(?<columns>.*?)\s+FROM\s+\w+\s+ORDER\s+BY\s+(?<column>\w+)\s*<->\s*'(?<vector>(?:''|[^'])*)'\s+LIMIT\s+(?<limit>\d+)\s*;?\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success) throw new SqlExecutionException("Vector search syntax is SELECT key, value FROM table ORDER BY embedding <-> '[...]' LIMIT k.");
        var schema = await _database.GetVectorSchemaAsync(table) ?? throw new SqlExecutionException($"Table '{table}' is not a vector table.");
        if (!string.Equals(match.Groups["column"].Value, schema.EmbeddingColumn, StringComparison.OrdinalIgnoreCase))
            throw new SqlExecutionException($"Vector search must order by '{schema.EmbeddingColumn}'.");
        if (!int.TryParse(match.Groups["limit"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var limit) || limit is < 1 or > 10_000)
            throw new SqlExecutionException("Vector search LIMIT must be between 1 and 10000.");
        double[] query;
        try { query = schema.ReadVector("{\"" + schema.EmbeddingColumn + "\":" + match.Groups["vector"].Value.Replace("''", "'") + "}"); }
        catch (ArgumentException ex) { throw new SqlExecutionException(ex.Message); }
        var nearest = await _database.SearchVectorsAsync(table, query, limit, Math.Max(64, limit));
        var rows = new List<IReadOnlyDictionary<string, string>>();
        foreach (var item in nearest)
        {
            var row = await _database.GetAsync(table, item.Id);
            if (row is null) continue;
            rows.Add(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["key"] = row.Key,
                ["value"] = row.Value,
                ["distance"] = item.Distance.ToString("R", CultureInfo.InvariantCulture)
            });
        }
        return SqlExecutionResult.WithRows("VECTOR SEARCH", rows);
    }

    private async Task<SqlExecutionResult> InsertAsync(string sql, string table, RelationalTableSchema schema, Guid? transactionId)
    {
        var match = Regex.Match(sql, @"^\s*INSERT\s+INTO\s+\w+\s*(?:\((?<columns>[^)]*)\))?\s+VALUES\s*\((?<values>.*)\)\s*;?\s*$", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success) throw new SqlExecutionException("Invalid relational INSERT syntax.");
        var columns = match.Groups["columns"].Success
            ? SplitCsv(match.Groups["columns"].Value).Select(value => value.Trim().ToLowerInvariant()).ToList()
            : schema.Columns.Select(column => column.Name.ToLowerInvariant()).ToList();
        var values = SplitCsv(match.Groups["values"].Value);
        if (columns.Count != values.Count) throw new SqlExecutionException("INSERT column count must match value count.");
        var row = BuildRow(schema, columns, values, out var key);
        if (transactionId is Guid id)
        {
            if (!_transactions.TryStagePut(id, table, key, row, out _)) throw new SqlExecutionException("transaction not found", 404);
        }
        else await _database.PutAsync(table, key, row);
        return SqlExecutionResult.Acknowledged("INSERT", 1, transactionId);
    }

    private async Task<SqlExecutionResult> SelectAsync(string sql, string table, RelationalTableSchema schema)
    {
        var match = Regex.Match(sql, @"^\s*SELECT\s+(?<columns>.*?)\s+FROM\s+\w+(?:\s+WHERE\s+(?<where>.*?))?(?:\s+LIMIT\s+(?<limit>\d+))?\s*;?\s*$", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success) throw new SqlExecutionException("Invalid relational SELECT syntax.");
        var columns = match.Groups["columns"].Value.Trim() == "*"
            ? schema.Columns.Select(column => column.Name).ToList()
            : SplitCsv(match.Groups["columns"].Value).Select(column => column.Trim().ToLowerInvariant()).ToList();
        var rows = await _database.RangeAsync(table, null, null, 10_000);
        if (match.Groups["where"].Success && !string.IsNullOrWhiteSpace(match.Groups["where"].Value))
        {
            var where = Regex.Match(match.Groups["where"].Value.Trim(), @"^(?<column>\w+)\s*=\s*(?<value>.+)$", RegexOptions.IgnoreCase);
            if (!where.Success) throw new SqlExecutionException("Relational WHERE currently supports column = value.");
            var column = where.Groups["column"].Value.ToLowerInvariant();
            var expected = FormatLiteral(where.Groups["value"].Value.Trim());
            rows = rows.Where(row => string.Equals(ReadColumn(row, schema, column), expected, StringComparison.OrdinalIgnoreCase)).ToList();
        }
        var limit = match.Groups["limit"].Success ? int.Parse(match.Groups["limit"].Value, CultureInfo.InvariantCulture) : 100;
        var projected = rows.Take(Math.Clamp(limit, 1, 1_000)).Select(row =>
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var column in columns) result[column] = ReadColumn(row, schema, column);
            return (IReadOnlyDictionary<string, string>)result;
        }).ToList();
        return SqlExecutionResult.WithRows("SELECT", projected);
    }

    private async Task<SqlExecutionResult> UpdateAsync(string sql, string table, RelationalTableSchema schema, Guid? transactionId)
    {
        var match = Regex.Match(sql, @"^\s*UPDATE\s+\w+\s+SET\s+(?<set>.*?)\s+WHERE\s+(?<column>\w+)\s*=\s*(?<key>.+?)\s*;?\s*$", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success) throw new SqlExecutionException("Invalid relational UPDATE syntax.");
        var keyColumn = schema.PrimaryKey.Name.ToLowerInvariant();
        if (!string.Equals(match.Groups["column"].Value, keyColumn, StringComparison.OrdinalIgnoreCase)) throw new SqlExecutionException("Relational UPDATE must target the primary key.");
        var key = FormatLiteral(match.Groups["key"].Value.Trim());
        var current = await _database.GetAsync(table, key) ?? throw new SqlExecutionException("row not found", 404);
        using var document = JsonDocument.Parse(current.Value);
        var values = document.RootElement.EnumerateObject().ToDictionary(property => property.Name, property => property.Value.Clone(), StringComparer.OrdinalIgnoreCase);
        foreach (var assignment in SplitCsv(match.Groups["set"].Value))
        {
            var parts = assignment.Split('=', 2);
            if (parts.Length != 2) throw new SqlExecutionException("Invalid relational assignment.");
            var column = parts[0].Trim();
            if (string.Equals(column, keyColumn, StringComparison.OrdinalIgnoreCase)) throw new SqlExecutionException("Primary key cannot be updated.");
            var definition = schema.Columns.FirstOrDefault(item => string.Equals(item.Name, column, StringComparison.OrdinalIgnoreCase)) ?? throw new SqlExecutionException($"Unknown column '{column}'.");
            values[definition.Name] = ParseJsonValue(parts[1].Trim(), definition.Type);
        }
        var json = JsonSerializer.Serialize(values.ToDictionary(item => item.Key, item => item.Value));
        if (transactionId is Guid id)
        {
            if (!_transactions.TryStagePut(id, table, key, json, out _)) throw new SqlExecutionException("transaction not found", 404);
        }
        else await _database.PutAsync(table, key, json);
        return SqlExecutionResult.Acknowledged("UPDATE", 1, transactionId);
    }

    private async Task<SqlExecutionResult> DeleteAsync(string sql, string table, RelationalTableSchema schema, Guid? transactionId)
    {
        var match = Regex.Match(sql, @"^\s*DELETE\s+FROM\s+\w+\s+WHERE\s+(?<column>\w+)\s*=\s*(?<key>.+?)\s*;?\s*$", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success || !string.Equals(match.Groups["column"].Value, schema.PrimaryKey.Name, StringComparison.OrdinalIgnoreCase)) throw new SqlExecutionException("Relational DELETE must target the primary key.");
        var key = FormatLiteral(match.Groups["key"].Value.Trim());
        if (transactionId is Guid id)
        {
            if (!_transactions.TryStageDelete(id, table, key, out _)) throw new SqlExecutionException("transaction not found", 404);
        }
        else await _database.DeleteAsync(table, key);
        return SqlExecutionResult.Acknowledged("DELETE", 1, transactionId);
    }

    private static string BuildRow(RelationalTableSchema schema, IReadOnlyList<string> columns, IReadOnlyList<string> literals, out string key)
    {
        var values = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
        key = string.Empty;
        for (var i = 0; i < columns.Count; i++)
        {
            var column = schema.Columns.FirstOrDefault(item => string.Equals(item.Name, columns[i], StringComparison.OrdinalIgnoreCase)) ?? throw new SqlExecutionException($"Unknown column '{columns[i]}'.");
            var literal = FormatLiteral(literals[i]);
            if (column.IsPrimaryKey) key = literal;
            else values[column.Name] = ParseJsonValue(literals[i], column.Type);
        }
        if (string.IsNullOrWhiteSpace(key)) throw new SqlExecutionException("Relational INSERT requires the primary key.");
        return JsonSerializer.Serialize(values);
    }

    private static JsonElement ParseJsonValue(string literal, RelationalColumnType type)
    {
        var value = literal.Trim();
        if (type == RelationalColumnType.Text) return JsonDocument.Parse(JsonSerializer.Serialize(Unquote(value))).RootElement.Clone();
        if (type == RelationalColumnType.Boolean) return JsonDocument.Parse(value.ToLowerInvariant()).RootElement.Clone();
        if (value.Equals("NULL", StringComparison.OrdinalIgnoreCase)) return JsonDocument.Parse("null").RootElement.Clone();
        return JsonDocument.Parse(value).RootElement.Clone();
    }

    private static string ReadColumn(KeyValueRow row, RelationalTableSchema schema, string column)
    {
        if (string.Equals(schema.PrimaryKey.Name, column, StringComparison.OrdinalIgnoreCase)) return row.Key;
        var definition = schema.Columns.FirstOrDefault(item => string.Equals(item.Name, column, StringComparison.OrdinalIgnoreCase)) ?? throw new SqlExecutionException($"Unknown column '{column}'.");
        using var document = JsonDocument.Parse(row.Value);
        return document.RootElement.TryGetProperty(definition.Name, out var value) ? value.ToString() : string.Empty;
    }

    private static string FormatLiteral(string literal) => Unquote(literal.Trim());
    private static string Unquote(string value) => value.Length >= 2 && value[0] == '\'' && value[^1] == '\'' ? value[1..^1].Replace("''", "'") : value;
    private static IReadOnlyList<string> SplitCsv(string value)
    {
        var parts = new List<string>(); var start = 0; var quoted = false;
        for (var i = 0; i < value.Length; i++)
        {
            if (value[i] == (char)39) quoted = !quoted;
            else if (value[i] == ',' && !quoted)
            {
                parts.Add(value[start..i]);
                start = i + 1;
            }
        }
        parts.Add(value[start..]); return parts;
    }
}

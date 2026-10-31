using LsmWriteDb.Transactions;
using System.Text.RegularExpressions;
using LsmWriteDb.Storage;

namespace LsmWriteDb.Sql;

internal sealed class SqlParser
{
    private readonly IReadOnlyList<SqlToken> _tokens;
    private int _position;

    private SqlParser(IReadOnlyList<SqlToken> tokens)
    {
        _tokens = tokens;
    }

    public static SqlStatement Parse(string sql)
    {
        var viewMatch = Regex.Match(sql.Trim(), @"^CREATE\s+VIEW\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s+AS\s+(?<query>SELECT\s+.+?)\s*;?\s*$", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (viewMatch.Success)
        {
            var query = viewMatch.Groups["query"].Value.Trim();
            if (!Regex.IsMatch(query, @"^SELECT\b", RegexOptions.IgnoreCase))
                throw new SqlParseException("A view definition must be a SELECT statement.");
            return new SqlCreateViewStatement(viewMatch.Groups["name"].Value.ToLowerInvariant(), query);
        }

        var parser = new SqlParser(SqlTokenizer.Tokenize(sql));
        var statement = parser.ParseStatement();
        parser.MatchSymbol(";");
        parser.ExpectEnd();
        return statement;
    }

    private SqlStatement ParseStatement()
    {
        if (MatchKeyword("BEGIN"))
        {
            MatchKeyword("TRANSACTION");
            var isolation = IsolationLevel.ReadCommitted;
            if (MatchKeyword("ISOLATION"))
            {
                ExpectKeyword("LEVEL");
                var first = ExpectIdentifier().ToUpperInvariant();
                var level = first;
                if (first is "READ" or "REPEATABLE") level += " " + ExpectIdentifier().ToUpperInvariant();
                isolation = IsolationLevelParser.Parse(level);
            }
            return new SqlBeginStatement(isolation);
        }

        if (MatchKeyword("COMMIT"))
        {
            MatchKeyword("TRANSACTION");
            return new SqlCommitStatement();
        }

        if (MatchKeyword("ROLLBACK"))
        {
            MatchKeyword("TRANSACTION");
            return new SqlRollbackStatement();
        }

        if (MatchKeyword("SHOW"))
        {
            ExpectKeyword("TABLES");
            return new SqlShowTablesStatement();
        }

        if (MatchKeyword("SEARCH"))
        {
            return ParseSearch();
        }

        if (MatchKeyword("CREATE"))
        {
            return ParseCreate();
        }

        if (MatchKeyword("DROP"))
        {
            return ParseDrop();
        }

        if (MatchKeyword("INSERT"))
        {
            return ParseInsert();
        }

        if (MatchKeyword("SELECT"))
        {
            return ParseSelect();
        }

        if (MatchKeyword("UPDATE"))
        {
            return ParseUpdate();
        }

        if (MatchKeyword("DELETE"))
        {
            return ParseDelete();
        }

        throw Error("Expected BEGIN, COMMIT, ROLLBACK, SHOW TABLES, CREATE, INSERT, SELECT, UPDATE, DELETE, or CREATE VIEW.");
    }

    private SqlStatement ParseDrop()
    {
        ExpectKeyword("TABLE");
        var ifExists = false;
        if (MatchKeyword("IF"))
        {
            ExpectKeyword("EXISTS");
            ifExists = true;
        }
        return new SqlDropTableStatement(ExpectTableName(), ifExists);
    }
    private SqlSearchStatement ParseSearch()
    {
        var index = ExpectIdentifier();
        ExpectKeyword("MATCH");
        var query = ExpectStringLiteral();
        var op = "or";
        if (MatchKeyword("OPERATOR"))
        {
            op = ExpectIdentifier().ToLowerInvariant();
            if (op is not ("and" or "or")) throw Error("SEARCH OPERATOR must be AND or OR.");
        }
        var limit = 20;
        if (MatchKeyword("LIMIT")) limit = ExpectPositiveNumber();
        return new SqlSearchStatement(index, query, op, limit);
    }
    private SqlStatement ParseCreate()
    {
        if (MatchKeyword("SEARCH"))
        {
            ExpectKeyword("INDEX");
            return ParseCreateSearchIndex();
        }

        if (MatchKeyword("RELATIONAL"))
        {
            ExpectKeyword("TABLE");
            return ParseCreateTable(relational: true);
        }

        if (MatchKeyword("TABLE"))
        {
            if (MatchKeyword("VECTOR")) return ParseCreateVectorTable();
            return ParseCreateTable();
        }

        if (MatchKeyword("INDEX"))
        {
            return ParseCreateIndex();
        }

        throw Error("Expected TABLE, RELATIONAL TABLE, or INDEX.");
    }
    private SqlCreateSearchIndexStatement ParseCreateSearchIndex()
    {
        var name = ExpectIndexName();
        ExpectKeyword("ON");
        var table = ExpectTableName();
        ExpectSymbol("(");
        var fields = new List<string>();
        do
        {
            var parts = new List<string> { ExpectIdentifier() };
            while (MatchSymbol(".")) parts.Add(ExpectIdentifier());
            fields.Add(string.Join('.', parts));
        }
        while (MatchSymbol(","));
        ExpectSymbol(")");
        return new SqlCreateSearchIndexStatement(table, name, fields);
    }
    private SqlCreateTableStatement ParseCreateTable(bool relational = false)
    {
        if (MatchKeyword("IF"))
        {
            ExpectKeyword("NOT");
            ExpectKeyword("EXISTS");
        }

        var table = ExpectTableName();
        if (!relational && !MatchSymbol("("))
            return new SqlCreateTableStatement(table);

        if (!relational)
            _position--;

        ExpectSymbol("(");
        var columns = new List<RelationalColumnDefinition>();
        do
        {
            var name = ExpectIdentifier().ToLowerInvariant();
            var typeName = ExpectIdentifier();
            var normalizedType = typeName.ToUpperInvariant();
            var type = normalizedType switch
            {
                "TEXT" or "CHAR" or "VARCHAR" or "VARCHAR2" => RelationalColumnType.Text,
                "INT" or "INTEGER" => RelationalColumnType.Int,
                "BIGINT" => RelationalColumnType.BigInt,
                "BOOL" or "BOOLEAN" => RelationalColumnType.Boolean,
                "DOUBLE" or "FLOAT" or "REAL" or "DECIMAL" or "NUMERIC" => RelationalColumnType.Double,
                _ => throw Error($"Unknown relational column type {typeName}.")
            };
            if (MatchSymbol("("))
            {
                ExpectPositiveNumber();
                ExpectSymbol(")");
            }

            var primaryKey = false;
            var nullable = true;
            if (MatchKeyword("PRIMARY"))
            {
                ExpectKeyword("KEY");
                primaryKey = true;
                nullable = false;
            }
            if (MatchKeyword("NOT"))
            {
                ExpectKeyword("NULL");
                nullable = false;
            }
            columns.Add(new RelationalColumnDefinition(name, type, primaryKey, nullable));
        }
        while (MatchSymbol(","));
        ExpectSymbol(")");

        var schema = new RelationalTableSchema(table, columns);
        try { schema.ValidateDefinition(); }
        catch (RelationalSchemaException ex) { throw new SqlParseException(ex.Message); }
        return new SqlCreateTableStatement(table, schema);
    }

    private SqlCreateTableStatement ParseCreateVectorTable()
    {
        var table = ExpectTableName();
        ExpectSymbol("(");
        var embeddingColumn = ExpectIdentifier().ToLowerInvariant();
        ExpectKeyword("VECTOR");
        ExpectSymbol("(");
        var dimensions = ExpectPositiveNumber();
        ExpectSymbol(")");
        ExpectSymbol(")");
        var schema = new VectorTableSchema(table, embeddingColumn, dimensions);
        try { schema.ValidateDefinition(); }
        catch (ArgumentException ex) { throw new SqlParseException(ex.Message); }
        return new SqlCreateTableStatement(table, VectorSchema: schema);
    }
    private SqlCreateIndexStatement ParseCreateIndex()
    {
        if (MatchKeyword("IF"))
        {
            ExpectKeyword("NOT");
            ExpectKeyword("EXISTS");
        }

        var name = ExpectIndexName();
        ExpectKeyword("ON");
        var table = ExpectTableName();
        ExpectSymbol("(");
        ExpectIdentifier("value");

        var path = new List<string>();
        if (MatchSymbol("."))
        {
            path.Add(ExpectIdentifier());
            while (MatchSymbol("."))
            {
                path.Add(ExpectIdentifier());
            }
        }

        ExpectSymbol(")");
        var storage = "btree";
        if (MatchKeyword("USING")) storage = ExpectIdentifier().ToLowerInvariant();
        if (storage is not ("btree" or "fastwrite")) throw Error("CREATE INDEX USING must be BTREE or FASTWRITE.");
        return new SqlCreateIndexStatement(table, name, path);
    }

    private SqlInsertStatement ParseInsert()
    {
        ExpectKeyword("INTO");
        var table = ExpectTableName();

        var columns = new List<string>();
        if (MatchSymbol("("))
        {
            do
            {
                columns.Add(ExpectColumnName());
            }
            while (MatchSymbol(","));

            ExpectSymbol(")");
        }
        else
        {
            columns.Add("key");
            columns.Add("value");
        }

        ExpectKeyword("VALUES");
        ExpectSymbol("(");

        var values = new List<string>();
        do
        {
            values.Add(ExpectStringLiteral());
        }
        while (MatchSymbol(","));

        ExpectSymbol(")");

        if (columns.Count != values.Count)
        {
            throw Error("INSERT column count must match value count.");
        }

        var row = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columns.Count; i++)
        {
            row[columns[i]] = values[i];
        }

        if (!row.TryGetValue("key", out var key) || !row.TryGetValue("value", out var value))
        {
            throw Error("INSERT requires key and value columns.");
        }

        return new SqlInsertStatement(table, key, value);
    }

    private SqlSelectStatement ParseSelect()
    {
        var columns = ParseSelectColumns();
        ExpectKeyword("FROM");
        var table = ExpectTableName();

        SqlJoinClause? join = null;
        if (MatchKeyword("JOIN"))
        {
            var joinTable = ExpectTableName();
            ExpectKeyword("ON");
            var leftColumn = ExpectJoinColumn();
            ExpectSymbol("=");
            var rightColumn = ExpectJoinColumn();
            join = new SqlJoinClause(joinTable, leftColumn, rightColumn);
        }

        var where = SqlWhereClause.All;

        if (MatchKeyword("WHERE"))
        {
            where = ParseWhereClause();
        }

        var limit = 100;
        if (MatchKeyword("LIMIT"))
        {
            limit = ExpectPositiveNumber();
        }

        return new SqlSelectStatement(table, columns, where, limit, join);
    }

    private IReadOnlyList<string> ParseSelectColumns()
    {
        if (MatchSymbol("*"))
        {
            return ["key", "value"];
        }

        var columns = new List<string>();
        do
        {
            columns.Add(ExpectQualifiedColumn());
        }
        while (MatchSymbol(","));

        return columns;
    }

    private SqlWhereClause ParseWhereClause()
    {
        var where = SqlWhereClause.All;
        while (true)
        {
            where = MergeWhereClause(where, ParseWhereTerm());
            if (!MatchKeyword("AND"))
            {
                return where;
            }
        }
    }

    private SqlWhereClause ParseWhereTerm()
    {
        var identifier = ExpectIdentifier();
        if (string.Equals(identifier, "key", StringComparison.OrdinalIgnoreCase))
        {
            return ParseKeyWhereTerm();
        }

        if (string.Equals(identifier, "value", StringComparison.OrdinalIgnoreCase))
        {
            return ParseValueWhereTerm();
        }

        if (MatchKeyword("BETWEEN"))
        {
            throw Error("BETWEEN is supported only for key predicates.");
        }

        throw Error("WHERE supports key predicates and value JSON property predicates.");
    }

    private SqlWhereClause ParseKeyWhereTerm()
    {
        if (MatchKeyword("BETWEEN"))
        {
            var start = ExpectStringLiteral();
            ExpectKeyword("AND");
            var end = ExpectStringLiteral();
            return new SqlWhereClause(null, start, end, null);
        }

        if (MatchSymbol("="))
        {
            return new SqlWhereClause(ExpectStringLiteral(), null, null, null);
        }

        if (MatchSymbol(">="))
        {
            return new SqlWhereClause(null, ExpectStringLiteral(), null, null);
        }

        if (MatchSymbol("<="))
        {
            return new SqlWhereClause(null, null, ExpectStringLiteral(), null);
        }

        throw Error("WHERE supports key = 'value', key BETWEEN 'start' AND 'end', key >= 'start', key <= 'end', value = 'json', and value.property = 'value'.");
    }

    private SqlWhereClause ParseValueWhereTerm()
    {
        if (MatchSymbol("="))
        {
            return new SqlWhereClause(null, null, null, new SqlValuePredicate([], ExpectStringLiteral()));
        }

        ExpectSymbol(".");

        var path = new List<string> { ExpectIdentifier() };
        while (MatchSymbol("."))
        {
            path.Add(ExpectIdentifier());
        }

        ExpectSymbol("=");
        return new SqlWhereClause(null, null, null, new SqlValuePredicate(path, ExpectStringLiteral()));
    }

    private SqlWhereClause MergeWhereClause(SqlWhereClause current, SqlWhereClause next)
    {
        if (next.Key is not null)
        {
            if (current.HasKeyFilter)
            {
                throw Error("Duplicate key predicate in WHERE clause.");
            }

            return current with { Key = next.Key };
        }

        var merged = current;
        if (next.Start is not null)
        {
            if (current.Key is not null)
            {
                throw Error("Cannot combine key equality with key range predicates.");
            }

            if (current.Start is not null)
            {
                throw Error("Duplicate lower key bound in WHERE clause.");
            }

            merged = merged with { Start = next.Start };
        }

        if (next.End is not null)
        {
            if (current.Key is not null)
            {
                throw Error("Cannot combine key equality with key range predicates.");
            }

            if (current.End is not null)
            {
                throw Error("Duplicate upper key bound in WHERE clause.");
            }

            merged = merged with { End = next.End };
        }

        if (next.ValuePredicate is not null)
        {
            if (current.ValuePredicate is not null)
            {
                throw Error("Duplicate value predicate in WHERE clause.");
            }

            merged = merged with { ValuePredicate = next.ValuePredicate };
        }

        return merged;
    }

    private SqlUpdateStatement ParseUpdate()
    {
        var table = ExpectTableName();
        ExpectKeyword("SET");
        ExpectIdentifier("value");
        ExpectSymbol("=");
        var value = ExpectStringLiteral();
        ExpectKeyword("WHERE");
        ExpectIdentifier("key");
        ExpectSymbol("=");
        var key = ExpectStringLiteral();

        return new SqlUpdateStatement(table, key, value);
    }

    private SqlDeleteStatement ParseDelete()
    {
        ExpectKeyword("FROM");
        var table = ExpectTableName();
        ExpectKeyword("WHERE");
        ExpectIdentifier("key");
        ExpectSymbol("=");
        var key = ExpectStringLiteral();

        return new SqlDeleteStatement(table, key);
    }

    private string ExpectTableName()
    {
        return ExpectIdentifier().ToLowerInvariant();
    }

    private string ExpectIndexName()
    {
        return ExpectIdentifier().ToLowerInvariant();
    }

    private string ExpectJoinColumn()
    {
        var table = ExpectIdentifier().ToLowerInvariant();
        ExpectSymbol(".");
        var column = ExpectIdentifier().ToLowerInvariant();
        if (column == "key")
            return $"{table}.key";
        if (column != "value")
            throw Error("JOIN columns must be table.key or table.value.path.");

        var path = new List<string>();
        while (MatchSymbol("."))
            path.Add(ExpectIdentifier());
        if (path.Count == 0)
            throw Error("JSON property joins must specify a value path, for example table.value.customerId.");
        return $"{table}.value.{string.Join(".", path)}";
    }
    private string ExpectQualifiedColumn()
    {
        var first = ExpectIdentifier().ToLowerInvariant();
        if (!MatchSymbol("."))
        {
            if (first is not ("key" or "value")) throw Error("Only key and value columns are supported.");
            return first;
        }

        var second = ExpectIdentifier().ToLowerInvariant();
        if (second is not ("key" or "value")) throw Error("Only key and value columns are supported.");
        return $"{first}.{second}";
    }

    private string ExpectColumnName()
    {
        var column = ExpectIdentifier();
        if (!string.Equals(column, "key", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(column, "value", StringComparison.OrdinalIgnoreCase))
        {
            throw Error("Only key and value columns are supported.");
        }

        return column.ToLowerInvariant();
    }

    private void ExpectIdentifier(string expected)
    {
        var value = ExpectIdentifier();
        if (!string.Equals(value, expected, StringComparison.OrdinalIgnoreCase))
        {
            throw Error($"Expected {expected}.");
        }
    }

    private string ExpectIdentifier()
    {
        var token = Current;
        if (token.Kind != SqlTokenKind.Identifier)
        {
            throw Error("Expected identifier.");
        }

        _position++;
        return token.Value;
    }

    private string ExpectStringLiteral()
    {
        var token = Current;
        if (token.Kind != SqlTokenKind.String)
        {
            throw Error("Expected string literal.");
        }

        _position++;
        return token.Value;
    }

    private int ExpectPositiveNumber()
    {
        var token = Current;
        if (token.Kind != SqlTokenKind.Number || !int.TryParse(token.Value, out var value) || value <= 0)
        {
            throw Error("Expected positive number.");
        }

        _position++;
        return value;
    }

    private void ExpectKeyword(string keyword)
    {
        if (!MatchKeyword(keyword))
        {
            throw Error($"Expected {keyword}.");
        }
    }

    private bool MatchKeyword(string keyword)
    {
        if (Current.Kind == SqlTokenKind.Identifier
            && string.Equals(Current.Value, keyword, StringComparison.OrdinalIgnoreCase))
        {
            _position++;
            return true;
        }

        return false;
    }

    private void ExpectSymbol(string symbol)
    {
        if (!MatchSymbol(symbol))
        {
            throw Error($"Expected {symbol}.");
        }
    }

    private bool MatchSymbol(string symbol)
    {
        if (Current.Kind == SqlTokenKind.Symbol
            && string.Equals(Current.Value, symbol, StringComparison.Ordinal))
        {
            _position++;
            return true;
        }

        return false;
    }

    private void ExpectEnd()
    {
        if (Current.Kind != SqlTokenKind.End)
        {
            throw Error("Unexpected token after end of statement.");
        }
    }

    private SqlParseException Error(string message)
    {
        return new SqlParseException($"{message} Near '{Current.Value}'.");
    }

    private SqlToken Current => _tokens[_position];
}

internal enum SqlTokenKind
{
    Identifier,
    String,
    Number,
    Symbol,
    End
}

internal sealed record SqlToken(SqlTokenKind Kind, string Value);

internal static class SqlTokenizer
{
    public static IReadOnlyList<SqlToken> Tokenize(string sql)
    {
        var tokens = new List<SqlToken>();
        var index = 0;

        while (index < sql.Length)
        {
            var current = sql[index];
            if (char.IsWhiteSpace(current))
            {
                index++;
                continue;
            }

            if (char.IsLetter(current) || current == '_')
            {
                var start = index;
                index++;
                while (index < sql.Length && (char.IsLetterOrDigit(sql[index]) || sql[index] == '_'))
                {
                    index++;
                }

                tokens.Add(new SqlToken(SqlTokenKind.Identifier, sql[start..index]));
                continue;
            }

            if (char.IsDigit(current))
            {
                var start = index;
                index++;
                while (index < sql.Length && char.IsDigit(sql[index]))
                {
                    index++;
                }

                tokens.Add(new SqlToken(SqlTokenKind.Number, sql[start..index]));
                continue;
            }

            if (current == '\'')
            {
                tokens.Add(new SqlToken(SqlTokenKind.String, ReadString(sql, ref index)));
                continue;
            }

            if ((current == '>' || current == '<') && index + 1 < sql.Length && sql[index + 1] == '=')
            {
                tokens.Add(new SqlToken(SqlTokenKind.Symbol, sql[index..(index + 2)]));
                index += 2;
                continue;
            }

            if (current is '(' or ')' or ',' or ';' or '*' or '=' or '.')
            {
                tokens.Add(new SqlToken(SqlTokenKind.Symbol, current.ToString()));
                index++;
                continue;
            }

            throw new SqlParseException($"Unexpected character '{current}'.");
        }

        tokens.Add(new SqlToken(SqlTokenKind.End, "<end>"));
        return tokens;
    }

    private static string ReadString(string sql, ref int index)
    {
        index++;
        var value = new System.Text.StringBuilder();

        while (index < sql.Length)
        {
            var current = sql[index];
            if (current == '\'')
            {
                if (index + 1 < sql.Length && sql[index + 1] == '\'')
                {
                    value.Append('\'');
                    index += 2;
                    continue;
                }

                index++;
                return value.ToString();
            }

            value.Append(current);
            index++;
        }

        throw new SqlParseException("Unterminated string literal.");
    }
}

using LsmWriteDb.Storage;
using LsmWriteDb.Raft;
using LsmWriteDb.Transactions;
using LsmWriteDb.Indexes;
using LsmWriteDb.Search;
using Microsoft.AspNetCore.Http;
using System.Globalization;

namespace LsmWriteDb.Sql;

public sealed class SqlEngine
{
    private readonly DatabaseEngine? _database;
    private readonly LsmStore? _singleStore;
    private readonly TransactionManager _transactions;
    private readonly RaftRoleGuard? _roleGuard;
    private readonly TableRaftRoleGuard? _tableRoleGuard;
    private readonly DistributedTransactionManager? _distributedTransactions;
    private readonly TableRaftCoordinator? _tableCoordinator;
    private readonly AsyncLocal<int> _viewDepth = new();

    public SqlEngine(LsmStore store, TransactionManager transactions)
        : this(store, transactions, roleGuard: null)
    {
    }

    public SqlEngine(LsmStore store, TransactionManager transactions, RaftRoleGuard? roleGuard)
    {
        _singleStore = store;
        _transactions = transactions;
        _roleGuard = roleGuard;
    }

    public SqlEngine(DatabaseEngine database, TransactionManager transactions)
        : this(database, transactions, roleGuard: null)
    {
    }

    public SqlEngine(DatabaseEngine database, TransactionManager transactions, RaftRoleGuard? roleGuard, TableRaftRoleGuard? tableRoleGuard = null, DistributedTransactionManager? distributedTransactions = null, TableRaftCoordinator? tableCoordinator = null)
    {
        _database = database;
        _transactions = transactions;
        _roleGuard = roleGuard;
        _tableRoleGuard = tableRoleGuard;
        _distributedTransactions = distributedTransactions;
        _tableCoordinator = tableCoordinator;
    }

    public async Task<SqlExecutionResult> ExecuteAsync(SqlQueryRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Query))
        {
            throw new SqlParseException("SQL query is required.");
        }

        if (_database is not null)
        {
            var relationalResult = await new RelationalSqlExecutor(_database, _transactions).TryExecuteAsync(request.Query, request.TransactionId);
            if (relationalResult is not null)
                return relationalResult;
        }

        var statement = SqlParser.Parse(request.Query);
        if (statement is not SqlSelectStatement
            and not SqlBeginStatement
            and not SqlCommitStatement
            and not SqlRollbackStatement
            and not SqlShowTablesStatement
            and not SqlCreateTableStatement
            and not SqlCreateViewStatement
            and not SqlDropTableStatement
            and not SqlSearchStatement)
        {
            if (_tableRoleGuard is not null)
                _tableRoleGuard.EnsureLeader(GetStatementTable(statement));
            else
                _roleGuard?.EnsureLeader();
        }

        return statement switch
        {
            SqlBeginStatement begin => Begin(begin.IsolationLevel),
            SqlCommitStatement => await CommitAsync(request.TransactionId),
            SqlRollbackStatement => Rollback(request.TransactionId),
            SqlShowTablesStatement => await ShowTablesAsync(),
            SqlCreateTableStatement create => await CreateTableAsync(create),
            SqlDropTableStatement drop => await DropTableAsync(drop),
            SqlCreateViewStatement createView => await CreateViewAsync(createView),
            SqlCreateIndexStatement createIndex => await CreateIndexAsync(createIndex),
            SqlCreateSearchIndexStatement createSearchIndex => await CreateSearchIndexAsync(createSearchIndex),
            SqlSearchStatement search => await SearchAsync(search, request.TransactionId),
            SqlInsertStatement insert => await InsertAsync(insert, request.TransactionId),
            SqlSelectStatement select => await SelectAsync(select, request.TransactionId),
            SqlUpdateStatement update => await UpdateAsync(update, request.TransactionId),
            SqlDeleteStatement delete => await DeleteAsync(delete, request.TransactionId),
            _ => throw new SqlExecutionException("Unsupported SQL statement.")
        };
    }

    private static string GetStatementTable(SqlStatement statement)
    {
        return statement switch
        {
            SqlCreateTableStatement create => create.Table,
            SqlDropTableStatement drop => drop.Table,
            SqlCreateViewStatement createView => createView.Name,
            SqlCreateIndexStatement create => create.Table,
            SqlCreateSearchIndexStatement create => create.Table,
            SqlInsertStatement insert => insert.Table,
            SqlUpdateStatement update => update.Table,
            SqlDeleteStatement delete => delete.Table,
            _ => TableNames.Default
        };
    }
    private async Task<SqlExecutionResult> ShowTablesAsync()
    {
        var tableInfos = _database is not null
            ? await _database.ListTablesAsync()
            : [new TableInfo(TableNames.Default)];
        var rows = tableInfos
            .Select(table =>
            {
                var status = table.Kind == "view" ? null : _tableCoordinator?.GetStatus(table.Name);
                return (IReadOnlyDictionary<string, string>)new Dictionary<string, string>
                {
                    ["table"] = table.Name,
                    ["kind"] = table.Kind,
                    ["leader"] = status?.LeaderId ?? string.Empty,
                    ["leaderUrl"] = status?.LeaderUrl ?? string.Empty
                };
            })
            .ToList();
        return SqlExecutionResult.WithRows("SHOW TABLES", rows);
    }

    private SqlExecutionResult Begin(IsolationLevel isolationLevel)
    {
        var transaction = _transactions.Begin(isolationLevel);
        return SqlExecutionResult.Acknowledged(
            "BEGIN",
            rowsAffected: 0,
            transaction.TransactionId,
            "transaction started");
    }

    private async Task<SqlExecutionResult> CommitAsync(Guid? transactionId)
    {
        var id = RequireTransactionId(transactionId, "COMMIT");
        var localOperations = _transactions.GetOperations(id);
        var localWrites = localOperations?.Select(operation =>
            new DistributedWrite(operation.Table, operation.Key, operation.Value, operation.IsDeleted)).ToList()
            ?? [];
        var allWrites = _distributedTransactions is not null
            ? await _distributedTransactions.CollectTransactionOperationsAsync(id, localWrites, CancellationToken.None)
            : localWrites;

        if (allWrites is null)
            throw TransactionNotFound();

        if (_distributedTransactions is null)
        {
            var commit = await _transactions.CommitAsync(id);
            if (commit is null)
                throw TransactionNotFound();

            return SqlExecutionResult.Acknowledged("COMMIT", commit.OperationCount, id, "transaction committed");
        }

        if (allWrites.Count == 0)
        {
            _transactions.Rollback(id);
            return SqlExecutionResult.Acknowledged("COMMIT", 0, id, "transaction committed");
        }

        var participantUrls = _distributedTransactions is not null
            ? await _distributedTransactions.ResolveParticipantUrlsAsync(allWrites, CancellationToken.None)
            : null;

        if (_distributedTransactions is not null && participantUrls is { Count: > 0 })
        {
            var distributed = _distributedTransactions.Begin();
            foreach (var write in allWrites)
                _distributedTransactions.Stage(distributed.TransactionId, write, out _);
            var result = await _distributedTransactions.CommitAsync(distributed.TransactionId, CancellationToken.None);
            if (result is null || result.Status is "aborted" or "in-doubt")
                throw new SqlExecutionException($"Distributed transaction {result?.Status ?? "failed"}.");
            _transactions.Rollback(id);
            return SqlExecutionResult.Acknowledged("COMMIT", result.OperationCount, id, "distributed transaction committed");
        }

        throw TransactionNotFound();
    }
    private SqlExecutionResult Rollback(Guid? transactionId)
    {
        var id = RequireTransactionId(transactionId, "ROLLBACK");
        if (!_transactions.Rollback(id))
        {
            throw TransactionNotFound();
        }

        return SqlExecutionResult.Acknowledged("ROLLBACK", rowsAffected: 0, id, "transaction rolled back");
    }

    private async Task<SqlExecutionResult> InsertAsync(SqlInsertStatement statement, Guid? transactionId)
    {
        await EnsureWritableTableAsync(statement.Table);
        EnsureValidJsonValue(statement.Value);
        await ValidateRelationalWriteAsync(statement.Table, statement.Key, statement.Value);

        if (transactionId is Guid id)
        {
            if (!_transactions.TryStagePut(id, statement.Table, statement.Key, statement.Value, out _))
            {
                throw TransactionNotFound();
            }
        }
        else
        {
            await PutAsync(statement.Table, statement.Key, statement.Value);
        }

        return SqlExecutionResult.Acknowledged("INSERT", rowsAffected: 1, transactionId);
    }

    private async Task<SqlExecutionResult> SelectAsync(SqlSelectStatement statement, Guid? transactionId)
    {
        if (_database is not null && transactionId is null)
        {
            var view = await _database.GetViewAsync(statement.Table);
            if (view is not null)
            {
                if (_viewDepth.Value >= 16)
                    throw new SqlExecutionException("View expansion depth exceeded; recursive views are not supported.");

                _viewDepth.Value++;
                try
                {
                    var result = await ExecuteAsync(new SqlQueryRequest(view.Query, null));
                    var limit = Math.Clamp(statement.Limit, 1, 1_000);
                    return result with { Rows = result.Rows.Take(limit).ToList(), RowsAffected = Math.Min(result.RowsAffected, limit) };
                }
                finally
                {
                    _viewDepth.Value--;
                }
            }
        }
        if (statement.Join is not null)
        {
            if (transactionId is not null)
                throw new SqlExecutionException("JOIN is not supported inside a transaction.");
            return await SelectJoinAsync(statement);
        }

        IReadOnlyList<KeyValueRow> rows;
        var where = statement.Where;
        var scanLimit = where.ValuePredicate is null ? statement.Limit : 1_000;
        var indexedRows = transactionId is null && where.Key is null && where.ValuePredicate is not null
            ? await TrySelectRowsWithIndexAsync(statement.Table, where, statement.Limit)
            : null;

        if (indexedRows is not null)
        {
            rows = indexedRows;
        }
        else if (where.Key is not null)
        {
            var row = transactionId is Guid id
                ? await GetTransactionRowAsync(id, statement.Table, where.Key)
                : await GetAsync(statement.Table, where.Key);

            rows = row is null ? [] : [row];
        }
        else if (transactionId is Guid id)
        {
            var result = await _transactions.RangeAsync(id, statement.Table, where.Start, where.End, scanLimit);
            if (!result.FoundTransaction)
            {
                throw TransactionNotFound();
            }

            rows = result.Rows;
        }
        else
        {
            rows = await RangeAsync(statement.Table, where.Start, where.End, scanLimit);
        }

        if (where.ValuePredicate is not null)
        {
            rows = rows
                .Where(row => MatchesValuePredicate(row.Value, where.ValuePredicate))
                .ToList();
        }

        var projectedRows = rows
            .Take(Math.Clamp(statement.Limit, 1, 1_000))
            .Select(row => Project(row, statement.Columns))
            .ToList();

        return SqlExecutionResult.WithRows("SELECT", projectedRows, transactionId);
    }

    private async Task<SqlExecutionResult> SelectJoinAsync(SqlSelectStatement statement)
    {
        var join = statement.Join!;
        if (_database is null)
            throw new SqlExecutionException("JOIN requires the multi-table database engine.");

        var left = ParseJoinOperand(statement.Table, join.LeftColumn);
        var right = ParseJoinOperand(join.Table, join.RightColumn);
        var leftRows = await _database.RangeAsync(statement.Table, null, null, 1_000);
        var rightRows = await _database.RangeAsync(join.Table, null, null, 1_000);
        var rightByValue = new Dictionary<string, List<KeyValueRow>>(StringComparer.Ordinal);
        foreach (var row in rightRows)
        {
            var value = right.IsKey ? row.Key : JsonValueAccessor.TryReadComparableValue(row.Value, right.Path, out var extractedRight) ? extractedRight : null;
            if (value is not null)
            {
                if (!rightByValue.TryGetValue(value, out var matches))
                    rightByValue[value] = matches = [];
                matches.Add(row);
            }
        }

        var rows = new List<IReadOnlyDictionary<string, string>>();
        foreach (var leftRow in leftRows)
        {
            var leftValue = left.IsKey ? leftRow.Key :
                JsonValueAccessor.TryReadComparableValue(leftRow.Value, left.Path, out var extracted) ? extracted : null;
            if (leftValue is null || !rightByValue.TryGetValue(leftValue, out var matches))
                continue;

            foreach (var rightRow in matches)
            {
                var projected = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var columns = statement.Columns.SequenceEqual(["key", "value"], StringComparer.OrdinalIgnoreCase)
                    ? [$"{statement.Table}.key", $"{statement.Table}.value", $"{join.Table}.key", $"{join.Table}.value"]
                    : statement.Columns;
                foreach (var column in columns)
                {
                    var parts = column.Split('.', 2);
                    if (parts.Length != 2 || (parts[1] != "key" && parts[1] != "value"))
                        throw new SqlExecutionException("JOIN columns must be qualified, for example users.value.");
                    var row = string.Equals(parts[0], statement.Table, StringComparison.OrdinalIgnoreCase) ? leftRow :
                        string.Equals(parts[0], join.Table, StringComparison.OrdinalIgnoreCase) ? rightRow :
                        throw new SqlExecutionException($"Unknown JOIN table qualifier '{parts[0]}'.");
                    projected[column] = parts[1] == "key" ? row.Key : row.Value;
                }
                rows.Add(projected);
                if (rows.Count >= Math.Clamp(statement.Limit, 1, 1_000))
                    return SqlExecutionResult.WithRows("SELECT", rows);
            }
        }
        return SqlExecutionResult.WithRows("SELECT", rows);
    }

    private static (bool IsKey, IReadOnlyList<string> Path) ParseJoinOperand(string table, string operand)
    {
        var prefix = table + ".";
        if (!operand.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            throw new SqlExecutionException($"JOIN column '{operand}' does not belong to table '{table}'.");
        var remainder = operand[prefix.Length..];
        if (remainder == "key")
            return (true, []);
        if (!remainder.StartsWith("value.", StringComparison.OrdinalIgnoreCase))
            throw new SqlExecutionException("JOIN columns must be table.key or table.value.path.");
        return (false, remainder["value.".Length..].Split('.'));
    }
    private async Task<SqlExecutionResult> UpdateAsync(SqlUpdateStatement statement, Guid? transactionId)
    {
        await EnsureWritableTableAsync(statement.Table);
        EnsureValidJsonValue(statement.Value);
        await ValidateRelationalWriteAsync(statement.Table, statement.Key, statement.Value);

        if (transactionId is Guid id)
        {
            if (!_transactions.TryStagePut(id, statement.Table, statement.Key, statement.Value, out _))
            {
                throw TransactionNotFound();
            }
        }
        else
        {
            await PutAsync(statement.Table, statement.Key, statement.Value);
        }

        return SqlExecutionResult.Acknowledged("UPDATE", rowsAffected: 1, transactionId);
    }

    private async Task<SqlExecutionResult> DeleteAsync(SqlDeleteStatement statement, Guid? transactionId)
    {
        await EnsureWritableTableAsync(statement.Table);
        if (transactionId is Guid id)
        {
            if (!_transactions.TryStageDelete(id, statement.Table, statement.Key, out _))
            {
                throw TransactionNotFound();
            }
        }
        else
        {
            await DeleteAsync(statement.Table, statement.Key);
        }

        return SqlExecutionResult.Acknowledged("DELETE", rowsAffected: 1, transactionId);
    }

    private async Task<SqlExecutionResult> CreateSearchIndexAsync(SqlCreateSearchIndexStatement statement)
    {
        if (_database is null) throw new SqlExecutionException("CREATE SEARCH INDEX requires the multi-table database engine.");
        var created = await _database.CreateSearchIndexAsync(statement.Table, statement.Name, statement.Fields);
        if (created && _tableCoordinator is not null)
            await _tableCoordinator.EnsureSearchIndexOnPeersAsync(statement.Name, statement.Table, statement.Fields);
        return SqlExecutionResult.Acknowledged("CREATE SEARCH INDEX", created ? 1 : 0, message: created ? "search index created" : "search index already exists");
    }
    private async Task<SqlExecutionResult> SearchAsync(SqlSearchStatement statement, Guid? transactionId)
    {
        if (_database is null) throw new SqlExecutionException("SEARCH requires the multi-table database engine.");
        var result = await _database.SearchAsync(statement.Index, new SearchRequest(statement.Query, statement.Operator, statement.Limit));
        var rows = result.Hits.Select(hit => (IReadOnlyDictionary<string, string>)new Dictionary<string, string>
        {
            ["key"] = hit.Key,
            ["value"] = hit.Value,
            ["score"] = hit.Score.ToString("R", CultureInfo.InvariantCulture)
        }).ToList();
        return SqlExecutionResult.WithRows("SEARCH", rows, transactionId);
    }
    private async Task<SqlExecutionResult> CreateIndexAsync(SqlCreateIndexStatement statement)
    {
        if (_database is null)
        {
            throw new SqlExecutionException("CREATE INDEX requires the multi-table database engine.");
        }

        var created = string.Equals(statement.Storage, "fastwrite", StringComparison.OrdinalIgnoreCase)
            ? await _database.CreateSstableValueIndexAsync(statement.Table, statement.Name, statement.Path)
            : await _database.CreateJsonValueIndexAsync(statement.Table, statement.Name, statement.Path);
        return SqlExecutionResult.Acknowledged(
            "CREATE INDEX",
            rowsAffected: created ? 1 : 0,
            message: created ? "index created" : "index already exists");
    }

    private async Task<SqlExecutionResult> DropTableAsync(SqlDropTableStatement statement)
    {
        if (_database is null)
        {
            if (!statement.IfExists)
                throw new SqlExecutionException("DROP TABLE requires the multi-table database engine.");
            return SqlExecutionResult.Acknowledged("DROP TABLE", 0, message: "table does not exist");
        }

        bool dropped;
        try
        {
            dropped = await _database.DropTableAsync(statement.Table);
        }
        catch (TableNotFoundException) when (statement.IfExists)
        {
            dropped = false;
        }
        catch (ArgumentException ex)
        {
            throw new SqlExecutionException(ex.Message);
        }

        if (dropped && _tableCoordinator is not null)
        {
            _tableCoordinator.RemoveTable(statement.Table);
            await _tableCoordinator.DropTableOnPeersAsync(statement.Table);
        }

        if (!dropped && !statement.IfExists)
            throw new SqlExecutionException($"table '{TableNames.Normalize(statement.Table)}' not found", StatusCodes.Status404NotFound);

        return SqlExecutionResult.Acknowledged("DROP TABLE", dropped ? 1 : 0,
            message: dropped ? "table dropped" : "table does not exist");
    }
    private async Task<SqlExecutionResult> CreateViewAsync(SqlCreateViewStatement statement)
    {
        if (_database is null)
            throw new SqlExecutionException("CREATE VIEW requires the multi-table database engine.");

        var created = await _database.CreateViewAsync(statement.Name, statement.Query);
        if (_tableCoordinator is not null)
            await _tableCoordinator.EnsureViewOnPeersAsync(statement.Name, statement.Query);
        return SqlExecutionResult.Acknowledged(
            "CREATE VIEW",
            rowsAffected: created ? 1 : 0,
            message: created ? "view created" : "view already exists");
    }
    private async Task<SqlExecutionResult> CreateTableAsync(SqlCreateTableStatement statement)
    {
        if (_database is null)
        {
            EnsureDefaultTable(statement.Table);
            return SqlExecutionResult.Acknowledged("CREATE TABLE", rowsAffected: 0, message: "table already exists");
        }

        var created = statement.VectorSchema is not null
            ? await _database.CreateVectorTableAsync(statement.VectorSchema)
            : statement.Schema is null
                ? await _database.CreateTableAsync(statement.Table)
                : await _database.CreateRelationalTableAsync(statement.Schema);
        if (_tableCoordinator is not null)
        {
            await _tableCoordinator.EnsureTableAsync(statement.Table);
            if (created)
            {
                if (statement.VectorSchema is not null)
                    await _tableCoordinator.EnsureVectorTableOnPeersAsync(statement.Table, statement.VectorSchema);
                else if (statement.Schema is null)
                    await _tableCoordinator.EnsureTableOnPeersAsync(statement.Table);
                else
                    await _tableCoordinator.EnsureRelationalTableOnPeersAsync(statement.Table, statement.Schema);
            }

            var ready = await _tableCoordinator.WaitForLeaderAsync(statement.Table);
            if (ready is null)
                throw new SqlExecutionException("table leader election is not ready", StatusCodes.Status503ServiceUnavailable);
        }
        return SqlExecutionResult.Acknowledged(
            statement.VectorSchema is not null ? "CREATE VECTOR TABLE" : statement.Schema is null ? "CREATE TABLE" : "CREATE RELATIONAL TABLE",
            rowsAffected: created ? 1 : 0,
            message: created ? "table created" : "table already exists");
    }

    private async Task<KeyValueRow?> GetTransactionRowAsync(Guid transactionId, string table, string key)
    {
        var result = await _transactions.GetAsync(transactionId, table, key);
        if (!result.FoundTransaction)
        {
            throw TransactionNotFound();
        }

        return result.Row;
    }

    private async Task EnsureWritableTableAsync(string table)
    {
        if (_database is not null && await _database.GetViewAsync(table) is not null)
            throw new SqlExecutionException($"View '{table}' is read-only.");
    }
    private async Task ValidateRelationalWriteAsync(string table, string key, string value)
    {
        if (_database is null)
            return;
        try
        {
            await _database.ValidateWriteAsync(table, key, value);
        }
        catch (RelationalSchemaException ex)
        {
            throw new SqlExecutionException(ex.Message);
        }
        catch (ArgumentException ex)
        {
            throw new SqlExecutionException(ex.Message);
        }
    }

    private async Task PutAsync(string table, string key, string value)
    {
        if (_database is not null)
        {
            await _database.PutAsync(table, key, value);
            return;
        }

        EnsureDefaultTable(table);
        await _singleStore!.PutAsync(key, value);
    }

    private async Task DeleteAsync(string table, string key)
    {
        if (_database is not null)
        {
            await _database.DeleteAsync(table, key);
            return;
        }

        EnsureDefaultTable(table);
        await _singleStore!.DeleteAsync(key);
    }

    private async Task<KeyValueRow?> GetAsync(string table, string key)
    {
        if (_database is not null)
        {
            return await _database.GetAsync(table, key);
        }

        EnsureDefaultTable(table);
        return await _singleStore!.GetAsync(key);
    }

    private async Task<IReadOnlyList<KeyValueRow>> RangeAsync(string table, string? start, string? end, int limit)
    {
        if (_database is not null)
        {
            return await _database.RangeAsync(table, start, end, limit);
        }

        EnsureDefaultTable(table);
        return await _singleStore!.RangeAsync(start, end, limit);
    }

    private async Task<IReadOnlyList<KeyValueRow>?> TrySelectRowsWithIndexAsync(
        string table,
        SqlWhereClause where,
        int limit)
    {
        if (_database is null || where.ValuePredicate is null)
        {
            return null;
        }

        var keys = await _database.TrySearchJsonValueIndexAsync(
            table,
            where.ValuePredicate.Path,
            where.ValuePredicate.Expected);
        keys ??= await _database.TrySearchSstableValueIndexAsync(
            table,
            where.ValuePredicate.Path,
            where.ValuePredicate.Expected);
        if (keys is null)
        {
            return null;
        }

        var boundedLimit = Math.Clamp(limit, 1, 1_000);
        var rows = new List<KeyValueRow>();
        foreach (var key in keys)
        {
            if (!IsInsideRange(key, where.Start, where.End))
            {
                continue;
            }

            var row = await _database.GetAsync(table, key);
            if (row is null || !MatchesValuePredicate(row.Value, where.ValuePredicate))
            {
                continue;
            }

            rows.Add(row);
            if (rows.Count == boundedLimit)
            {
                break;
            }
        }

        return rows;
    }

    private static IReadOnlyDictionary<string, string> Project(KeyValueRow row, IReadOnlyList<string> columns)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var column in columns)
        {
            values[column] = column switch
            {
                "key" => row.Key,
                "value" => row.Value,
                _ => throw new SqlExecutionException($"Unsupported column {column}.")
            };
        }

        return values;
    }

    private static void EnsureValidJsonValue(string value)
    {
        if (!JsonValueAccessor.IsValidJson(value, out var error))
        {
            throw new SqlExecutionException($"value must be valid JSON: {error}");
        }
    }

    private static bool MatchesValuePredicate(string value, SqlValuePredicate predicate)
    {
        return JsonValueAccessor.TryReadComparableValue(value, predicate.Path, out var actual)
            && string.Equals(actual, predicate.Expected, StringComparison.Ordinal);
    }

    private static bool IsInsideRange(string key, string? start, string? end)
    {
        if (start is not null && string.CompareOrdinal(key, start) < 0)
        {
            return false;
        }

        if (end is not null && string.CompareOrdinal(key, end) > 0)
        {
            return false;
        }

        return true;
    }

    private static Guid RequireTransactionId(Guid? transactionId, string statementType)
    {
        if (transactionId is not Guid id)
        {
            throw new SqlExecutionException($"{statementType} requires transactionId in the request body.");
        }

        return id;
    }

    private static SqlExecutionException TransactionNotFound()
    {
        return new SqlExecutionException("transaction not found", StatusCodes.Status404NotFound);
    }

    private static void EnsureDefaultTable(string table)
    {
        var normalized = TableNames.Normalize(table);
        if (!string.Equals(normalized, TableNames.Default, StringComparison.Ordinal))
        {
            throw new SqlExecutionException($"table '{normalized}' not found", StatusCodes.Status404NotFound);
        }
    }
}

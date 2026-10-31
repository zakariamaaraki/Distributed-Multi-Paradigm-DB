using LsmWriteDb.Transactions;
using Microsoft.AspNetCore.Http;
using LsmWriteDb.Storage;

namespace LsmWriteDb.Sql;

public sealed record SqlQueryRequest(string? Query, Guid? TransactionId);

public sealed record SqlExecutionResult(
    string StatementType,
    Guid? TransactionId,
    int RowsAffected,
    IReadOnlyList<IReadOnlyDictionary<string, string>> Rows,
    string? Message)
{
    public static SqlExecutionResult Acknowledged(
        string statementType,
        int rowsAffected,
        Guid? transactionId = null,
        string? message = null)
    {
        return new SqlExecutionResult(statementType, transactionId, rowsAffected, [], message);
    }

    public static SqlExecutionResult WithRows(
        string statementType,
        IReadOnlyList<IReadOnlyDictionary<string, string>> rows,
        Guid? transactionId = null)
    {
        return new SqlExecutionResult(statementType, transactionId, rows.Count, rows, null);
    }
}

internal abstract record SqlStatement(string StatementType);

internal sealed record SqlBeginStatement(IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted) : SqlStatement("BEGIN");

internal sealed record SqlCommitStatement() : SqlStatement("COMMIT");

internal sealed record SqlRollbackStatement() : SqlStatement("ROLLBACK");

internal sealed record SqlShowTablesStatement() : SqlStatement("SHOW TABLES");

internal sealed record SqlCreateTableStatement(string Table, RelationalTableSchema? Schema = null, VectorTableSchema? VectorSchema = null) : SqlStatement(VectorSchema is not null ? "CREATE VECTOR TABLE" : Schema is null ? "CREATE TABLE" : "CREATE RELATIONAL TABLE");

internal sealed record SqlDropTableStatement(string Table, bool IfExists = false) : SqlStatement("DROP TABLE");

internal sealed record SqlCreateViewStatement(string Name, string Query) : SqlStatement("CREATE VIEW");

internal sealed record SqlSearchStatement(string Index, string Query, string Operator, int Limit) : SqlStatement("SEARCH");
internal sealed record SqlCreateSearchIndexStatement(string Table, string Name, IReadOnlyList<string> Fields) : SqlStatement("CREATE SEARCH INDEX");

internal sealed record SqlCreateIndexStatement(
    string Table,
    string Name,
    IReadOnlyList<string> Path,
    string Storage = "btree") : SqlStatement("CREATE INDEX");

internal sealed record SqlInsertStatement(string Table, string Key, string Value) : SqlStatement("INSERT");

internal sealed record SqlSelectStatement(
    string Table,
    IReadOnlyList<string> Columns,
    SqlWhereClause Where,
    int Limit,
    SqlJoinClause? Join = null) : SqlStatement("SELECT");

internal sealed record SqlJoinClause(string Table, string LeftColumn, string RightColumn);

internal sealed record SqlWhereClause(
    string? Key,
    string? Start,
    string? End,
    SqlValuePredicate? ValuePredicate)
{
    public static SqlWhereClause All { get; } = new(null, null, null, null);

    public bool HasKeyFilter => Key is not null || Start is not null || End is not null;
}

internal sealed record SqlValuePredicate(IReadOnlyList<string> Path, string Expected);

internal sealed record SqlUpdateStatement(string Table, string Key, string Value) : SqlStatement("UPDATE");

internal sealed record SqlDeleteStatement(string Table, string Key) : SqlStatement("DELETE");

public sealed class SqlParseException : Exception
{
    public SqlParseException(string message)
        : base(message)
    {
    }
}

public sealed class SqlExecutionException : Exception
{
    public SqlExecutionException(string message, int statusCode = StatusCodes.Status400BadRequest)
        : base(message)
    {
        StatusCode = statusCode;
    }

    public int StatusCode { get; }
}

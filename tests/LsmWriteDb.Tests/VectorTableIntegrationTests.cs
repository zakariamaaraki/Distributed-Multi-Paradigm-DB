using System.Net;
using System.Net.Http.Json;
using LsmWriteDb.ChangeLogs;
using LsmWriteDb.Raft;
using LsmWriteDb.Sql;
using LsmWriteDb.Storage;
using LsmWriteDb.Transactions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

namespace LsmWriteDb.Tests;

public sealed class VectorTableIntegrationTests
{
    [Fact]
    public async Task Sql_CreatesVectorTableSearchesUpdatesDeletesAndRebuildsAfterRestart()
    {
        var dataPath = CreateTempDataPath();
        try
        {
            var engine = await CreateSqlEngineAsync(dataPath);
            var create = await engine.ExecuteAsync(new SqlQueryRequest(
                "CREATE TABLE VECTOR products (embedding VECTOR(3))", null));
            Assert.Equal("CREATE VECTOR TABLE", create.StatementType);
            Assert.Equal(1, create.RowsAffected);

            await InsertAsync(engine, "p1", "[1,0,0]");
            await InsertAsync(engine, "p2", "[0.8,0.2,0]");
            await InsertAsync(engine, "p3", "[0,1,0]");

            var pointRead = await engine.ExecuteAsync(new SqlQueryRequest(
                "SELECT key, value FROM products WHERE key = 'p2'", null));
            Assert.Equal("p2", Assert.Single(pointRead.Rows)["key"]);

            var result = await SearchAsync(engine, "[1,0,0]", 3);
            Assert.Equal("p1", result.Rows[0]["key"]);
            Assert.Equal("p2", result.Rows[1]["key"]);
            Assert.All(result.Rows, row => Assert.Contains("distance", row.Keys));

            await engine.ExecuteAsync(new SqlQueryRequest(
                "UPDATE products SET value = '{\"embedding\":[0,0,1]}' WHERE key = 'p1'", null));
            Assert.Equal("p2", (await SearchAsync(engine, "[1,0,0]", 1)).Rows[0]["key"]);

            await engine.ExecuteAsync(new SqlQueryRequest("DELETE FROM products WHERE key = 'p2'", null));
            Assert.Equal("p3", (await SearchAsync(engine, "[0,1,0]", 1)).Rows[0]["key"]);

            var tables = await engine.ExecuteAsync(new SqlQueryRequest("SHOW TABLES", null));
            Assert.Equal("vector", Assert.Single(tables.Rows, row => row["table"] == "products")["kind"]);

            var restored = await CreateSqlEngineAsync(dataPath);
            var afterRestart = await SearchAsync(restored, "[0,0,1]", 2);
            Assert.Equal("p1", afterRestart.Rows[0]["key"]);
            Assert.DoesNotContain(afterRestart.Rows, row => row["key"] == "p2");
        }
        finally { DeleteTempDataPath(dataPath); }
    }

    [Fact]
    public async Task Sql_ValidatesDimensionsAndRejectsInvalidVectorSearches()
    {
        var dataPath = CreateTempDataPath();
        try
        {
            var engine = await CreateSqlEngineAsync(dataPath);
            await engine.ExecuteAsync(new SqlQueryRequest("CREATE TABLE VECTOR vectors (embedding VECTOR(2))", null));

            var shortVector = await Assert.ThrowsAsync<SqlExecutionException>(() => engine.ExecuteAsync(new SqlQueryRequest(
                "INSERT INTO vectors VALUES ('short', '{\"embedding\":[1]}')", null)));
            Assert.Contains("2 dimensions", shortVector.Message);

            var wrongColumn = await Assert.ThrowsAsync<SqlExecutionException>(() => engine.ExecuteAsync(new SqlQueryRequest(
                "SELECT key, value FROM vectors ORDER BY other <-> '[1,0]' LIMIT 1", null)));
            Assert.Contains("embedding", wrongColumn.Message);

            var pointRead = await engine.ExecuteAsync(new SqlQueryRequest(
                "SELECT key, value FROM vectors WHERE key = 'x'", null));
            Assert.Empty(pointRead.Rows);

            var wrongQueryDimension = await Assert.ThrowsAsync<SqlExecutionException>(() => engine.ExecuteAsync(new SqlQueryRequest(
                "SELECT key, value FROM vectors ORDER BY embedding <-> '[1]' LIMIT 1", null)));
            Assert.Contains("2 dimensions", wrongQueryDimension.Message);

            var invalidLimit = await Assert.ThrowsAsync<SqlExecutionException>(() => engine.ExecuteAsync(new SqlQueryRequest(
                "SELECT key, value FROM vectors ORDER BY embedding <-> '[1,0]' LIMIT 0", null)));
            Assert.Contains("LIMIT", invalidLimit.Message);

            await Assert.ThrowsAsync<SqlParseException>(() => engine.ExecuteAsync(new SqlQueryRequest(
                "CREATE TABLE VECTOR bad (embedding VECTOR(0))", null)));
        }
        finally { DeleteTempDataPath(dataPath); }
    }

    [Fact]
    public async Task Sql_UpdatesVectorIndexOnlyAfterTransactionCommit()
    {
        var dataPath = CreateTempDataPath();
        try
        {
            var engine = await CreateSqlEngineAsync(dataPath);
            await engine.ExecuteAsync(new SqlQueryRequest("CREATE TABLE VECTOR products (embedding VECTOR(2))", null));
            var begin = await engine.ExecuteAsync(new SqlQueryRequest("BEGIN", null));
            var transactionId = begin.TransactionId!.Value;
            await engine.ExecuteAsync(new SqlQueryRequest(
                "INSERT INTO products VALUES ('tx-row', '{\"embedding\":[1,0]}')", transactionId));
            Assert.Empty((await SearchAsync(engine, "[1,0]", 1)).Rows);

            await engine.ExecuteAsync(new SqlQueryRequest("COMMIT", transactionId));
            Assert.Equal("tx-row", (await SearchAsync(engine, "[1,0]", 1)).Rows[0]["key"]);

            var rollback = await engine.ExecuteAsync(new SqlQueryRequest("BEGIN", null));
            await engine.ExecuteAsync(new SqlQueryRequest(
                "INSERT INTO products VALUES ('rolled-back', '{\"embedding\":[0,1]}')", rollback.TransactionId));
            await engine.ExecuteAsync(new SqlQueryRequest("ROLLBACK", rollback.TransactionId));
            Assert.DoesNotContain((await SearchAsync(engine, "[0,1]", 2)).Rows, row => row["key"] == "rolled-back");
        }
        finally { DeleteTempDataPath(dataPath); }
    }

    [Fact]
    public async Task Database_ReplicatedChangesUpdateVectorGraphAndValidatePayloads()
    {
        var dataPath = CreateTempDataPath();
        try
        {
            var database = await CreateDatabaseAsync(dataPath);
            await database.CreateVectorTableAsync(new VectorTableSchema("vectors", "embedding", 2));
            await database.ApplyReplicatedChangeAsync(new ChangeLogEntry(8, "put", "one", "{\"embedding\":[1,0]}", false, DateTimeOffset.UtcNow)
            {
                Table = "vectors"
            });
            Assert.Equal("one", Assert.Single(await database.SearchVectorsAsync("vectors", [1, 0], 1)).Id);

            await database.ApplyReplicatedChangeAsync(new ChangeLogEntry(9, "delete", "one", null, true, DateTimeOffset.UtcNow)
            {
                Table = "vectors"
            });
            Assert.Empty(await database.SearchVectorsAsync("vectors", [1, 0], 1));

            await Assert.ThrowsAsync<ArgumentException>(() => database.ApplyReplicatedChangeAsync(
                new ChangeLogEntry(10, "put", "bad", "{\"embedding\":[1]}", false, DateTimeOffset.UtcNow)
                { Table = "vectors" }));
            Assert.Null(await database.GetAsync("vectors", "bad"));
        }
        finally { DeleteTempDataPath(dataPath); }
    }

    [Fact]
    public async Task Http_VectorTableRouteAndSqlEndpointWorkTogether()
    {
        var dataPath = CreateTempDataPath();
        try
        {
            await using var app = await CreateAppAsync(dataPath);
            using var client = app.GetTestClient();
            var response = await client.PutAsJsonAsync("/tables/http_vectors/vector",
                new VectorTableSchema("http_vectors", "embedding", 2));
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var badSchema = await client.PutAsJsonAsync("/tables/http_vectors/vector",
                new VectorTableSchema("different_table", "embedding", 2));
            Assert.Equal(HttpStatusCode.BadRequest, badSchema.StatusCode);

            var create = await client.PostAsJsonAsync("/sql", new SqlQueryRequest(
                "INSERT INTO http_vectors VALUES ('http-row', '{\"embedding\":[1,0]}')", null));
            Assert.Equal(HttpStatusCode.OK, create.StatusCode);

            var search = await client.PostAsJsonAsync("/sql", new SqlQueryRequest(
                "SELECT key, value FROM http_vectors ORDER BY embedding <-> '[1,0]' LIMIT 1", null));
            Assert.Equal(HttpStatusCode.OK, search.StatusCode);
            var payload = await search.Content.ReadFromJsonAsync<SqlExecutionResult>();
            Assert.Equal("http-row", Assert.Single(payload!.Rows)["key"]);

            var invalidVector = await client.PostAsJsonAsync("/sql", new SqlQueryRequest(
                "INSERT INTO http_vectors VALUES ('bad', '{\"embedding\":[1]}')", null));
            Assert.Equal(HttpStatusCode.BadRequest, invalidVector.StatusCode);
        }
        finally { DeleteTempDataPath(dataPath); }
    }

    private static async Task InsertAsync(SqlEngine engine, string key, string vector) =>
        await engine.ExecuteAsync(new SqlQueryRequest(
            $"INSERT INTO products VALUES ('{key}', '{{\"embedding\":{vector}}}')", null));

    private static Task<SqlExecutionResult> SearchAsync(SqlEngine engine, string vector, int limit) =>
        engine.ExecuteAsync(new SqlQueryRequest(
            $"SELECT key, value FROM products ORDER BY embedding <-> '{vector}' LIMIT {limit}", null));

    private static async Task<SqlEngine> CreateSqlEngineAsync(string dataPath)
    {
        var database = await CreateDatabaseAsync(dataPath);
        return new SqlEngine(database, new TransactionManager(database));
    }

    private static async Task<DatabaseEngine> CreateDatabaseAsync(string dataPath)
    {
        var options = new LsmStoreOptions(dataPath, FlushThreshold: 2);
        var database = new DatabaseEngine(options, new ChangeLogService(options));
        await database.InitializeAsync();
        return database;
    }

    private static async Task<WebApplication> CreateAppAsync(string dataPath)
    {
        var options = new LsmStoreOptions(dataPath, FlushThreshold: 2);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton<ChangeLogService>();
        builder.Services.AddSingleton<DatabaseEngine>();
        builder.Services.AddSingleton<TransactionManager>();
        builder.Services.AddSingleton<SqlEngine>();
        builder.Services.AddSingleton(new RaftOptions { Enabled = false, NodeId = "test", PublicUrl = "http://test" });
        builder.Services.AddSingleton(new HttpClient { Timeout = Timeout.InfiniteTimeSpan });
        builder.Services.AddSingleton<TableRaftCoordinator>();
        builder.Services.AddSingleton<TableRaftRoleGuard>();
        var app = builder.Build();
        app.MapTableEndpoints();
        app.MapSqlEndpoints();
        await app.Services.GetRequiredService<DatabaseEngine>().InitializeAsync();
        await app.StartAsync();
        return app;
    }

    private static string CreateTempDataPath() => Path.Combine(Path.GetTempPath(), "LsmWriteDb.Tests", Guid.NewGuid().ToString("N"));

    private static void DeleteTempDataPath(string path)
    {
        if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
    }
}

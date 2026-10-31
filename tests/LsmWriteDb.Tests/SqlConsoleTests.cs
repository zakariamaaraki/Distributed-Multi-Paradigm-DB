namespace LsmWriteDb.Tests;

public sealed class SqlConsoleTests
{
    [Fact]
    public void StaticAssets_IncludeSqlConsoleShellAndSqlEndpoint()
    {
        var html = StaticAssetTestHelper.Read(Path.Combine("sql-console", "index.html"));
        var script = StaticAssetTestHelper.Read(Path.Combine("sql-console", "sql-console.js"));

        Assert.Contains("LsmWriteDb Console", html);
        Assert.Contains("id=\"queryEditor\"", html);
        Assert.Contains("id=\"runQuery\"", html);
        Assert.Contains("/sql-console/sql-console.css", html);
        Assert.Contains("/sql-console/sql-console.js", html);
        Assert.Contains("fetch('/sql'", script);
        Assert.Contains("X-Read-Consistency", script);
        Assert.Contains("id=\"transactionId\"", html);
        Assert.Contains("id=\"transactionIsolation\"", html);
        Assert.Contains("REPEATABLE READ", html);
        Assert.Contains("id=\"readConsistency\"", html);
        Assert.Contains("Strong (table leader)", html);
        Assert.Contains("/changes-console", html);
        Assert.Contains("ISOLATION LEVEL ${transactionIsolation.value}", script);
        Assert.Contains("ANSI: CREATE TABLE relational_users (...)", html);
        Assert.Contains("CREATE TABLE relational_users (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, active BOOLEAN)", html);
        Assert.Contains("SELECT id, name, active FROM relational_users WHERE id = 42", html);
        Assert.Contains("ANSI: DROP TABLE IF EXISTS relational_users", html);
        Assert.Contains("ANSI: CREATE VIEW gold_users AS ...", html);
        Assert.Contains("SELECT * FROM gold_users", html);
        Assert.Contains("CREATE INDEX idx_users_tier ON users (value.tier)", html);
        Assert.Contains("SELECT key, value FROM users WHERE value.tier = 'gold' LIMIT 50", html);
        Assert.Contains("Vector quickstart: click each Vector example in order", html);
        Assert.Contains("data-query=\"CREATE TABLE VECTOR demo_vectors (embedding VECTOR(3))\"", html);
        Assert.Contains("INSERT INTO demo_vectors VALUES ('item-1'", html);
        Assert.Contains("embedding &lt;-&gt; '[1,0,0]'", html);
        Assert.Contains("cosine distance", html);
    }
}

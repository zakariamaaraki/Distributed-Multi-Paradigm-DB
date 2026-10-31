using LsmWriteDb.Raft;
using Microsoft.AspNetCore.Mvc;

namespace LsmWriteDb.Storage;

public static class TableEndpoints
{
    public static WebApplication MapTableEndpoints(this WebApplication app)
    {
        app.MapGet("/tables", async (DatabaseEngine db, TableRaftCoordinator coordinator) =>
        {
            var tables = await db.ListTablesAsync();
            var result = tables.Select(table =>
            {
                var status = table.Kind == "view" ? null : coordinator.GetStatus(table.Name);
                return new
                {
                    name = table.Name,
                    table = table.Name,
                    kind = table.Kind,
                    leaderId = status?.LeaderId,
                    leaderUrl = status?.LeaderUrl,
                    role = status?.Role.ToString(),
                    term = status?.CurrentTerm
                };
            });
            return Results.Ok(result);
        });

        app.MapDelete("/tables/{table}", async (string table, DatabaseEngine db, TableRaftCoordinator coordinator, CancellationToken cancellationToken) =>
        {
            try
            {
                var normalized = TableNames.Normalize(table);
                var dropped = await db.DropTableAsync(normalized, cancellationToken);
                if (dropped)
                {
                    coordinator.RemoveTable(normalized);
                    await coordinator.DropTableOnPeersAsync(normalized, cancellationToken);
                }
                return dropped ? Results.NoContent() : Results.NotFound();
            }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });
        app.MapPut("/tables/{table}", async (string table, DatabaseEngine db, TableRaftCoordinator coordinator, CancellationToken cancellationToken) =>
        {
            try
            {
                var created = await db.CreateTableAsync(table, cancellationToken);
                await coordinator.EnsureTableAsync(table, cancellationToken);
                if (created)
                    await coordinator.EnsureTableOnPeersAsync(table, cancellationToken);

                var ready = await coordinator.WaitForLeaderAsync(table, cancellationToken);
                return ready is null
                    ? Results.Json(new { error = "table leader election is not ready" }, statusCode: StatusCodes.Status503ServiceUnavailable)
                    : created
                        ? Results.Created($"/tables/{TableNames.Normalize(table)}", new { table = TableNames.Normalize(table), leaderId = ready.LeaderId, leaderUrl = ready.LeaderUrl, term = ready.CurrentTerm })
                        : Results.Ok(new { table = TableNames.Normalize(table), message = "table already exists", leaderId = ready.LeaderId, leaderUrl = ready.LeaderUrl, term = ready.CurrentTerm });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        app.MapPut("/tables/{table}/relational", async (string table, RelationalTableSchema schema, DatabaseEngine db, TableRaftCoordinator coordinator, CancellationToken cancellationToken) =>
        {
            try
            {
                var normalized = TableNames.Normalize(table);
                if (!string.Equals(normalized, TableNames.Normalize(schema.Table), StringComparison.Ordinal))
                    return Results.BadRequest(new { error = "route table and schema table must match" });

                var created = await db.CreateRelationalTableAsync(schema with { Table = normalized }, cancellationToken);
                await coordinator.EnsureTableAsync(normalized, cancellationToken);
                var ready = await coordinator.WaitForLeaderAsync(normalized, cancellationToken);
                return ready is null
                    ? Results.Json(new { error = "table leader election is not ready" }, statusCode: StatusCodes.Status503ServiceUnavailable)
                    : Results.Ok(new { table = normalized, created });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        app.MapPut("/tables/{table}/vector", async (string table, VectorTableSchema schema, DatabaseEngine db, TableRaftCoordinator coordinator, CancellationToken cancellationToken) =>
        {
            try
            {
                var normalized = TableNames.Normalize(table);
                if (!string.Equals(normalized, TableNames.Normalize(schema.Table), StringComparison.Ordinal))
                    return Results.BadRequest(new { error = "route table and schema table must match" });
                await db.CreateVectorTableAsync(schema with { Table = normalized }, cancellationToken);
                await coordinator.EnsureTableAsync(normalized, cancellationToken);
                return Results.Ok(new { table = normalized, kind = "vector" });
            }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });
        app.MapGet("/views/{view}", async (string view, DatabaseEngine db, CancellationToken cancellationToken) =>
        {
            var definition = await db.GetViewAsync(view, cancellationToken);
            return definition is null ? Results.NotFound() : Results.Ok(definition);
        });
        app.MapPut("/views/{view}", async (string view, CreateViewRequest request, DatabaseEngine db, CancellationToken cancellationToken) =>
        {
            try
            {
                var normalized = TableNames.Normalize(view);
                var created = await db.CreateViewAsync(normalized, request.Query, cancellationToken);
                return Results.Ok(new { view = normalized, kind = "view", created });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });        MapKeyValueRoutes(app, "/kv", TableNames.Default);
        MapKeyValueRoutes(app, "/tables/{table}/kv", tableName: null);

        app.MapGet("/tables/{table}/snapshot", async (string table, DatabaseEngine db) =>
        {
            try { return Results.Ok(await db.GetSnapshotAsync(table)); }
            catch (TableNotFoundException ex) { return Results.NotFound(new { error = ex.Message }); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        app.MapGet("/stats", async (DatabaseEngine db) => Results.Ok(await db.GetStatsAsync()));

        app.MapGet("/tables/{table}/stats", async (string table, DatabaseEngine db) =>
        {
            try
            {
                return Results.Ok(await db.GetTableStatsAsync(table));
            }
            catch (TableNotFoundException ex)
            {
                return Results.NotFound(new { error = ex.Message });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        return app;
    }

    private static void MapKeyValueRoutes(WebApplication app, string routePrefix, string? tableName)
    {
        app.MapGet($"{routePrefix}/range", async (
            string? table,
            [FromQuery] string? start,
            [FromQuery] string? end,
            [FromQuery] int? limit,
            DatabaseEngine db) =>
        {
            if (start is not null && end is not null && string.CompareOrdinal(start, end) > 0)
            {
                return Results.BadRequest(new { error = "start must be less than or equal to end" });
            }

            try
            {
                var rows = await db.RangeAsync(tableName ?? table ?? TableNames.Default, start, end, limit ?? 100);
                return Results.Ok(rows);
            }
            catch (TableNotFoundException ex)
            {
                return Results.NotFound(new { error = ex.Message });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        app.MapGet($"{routePrefix}/{{key}}", async (string? table, string key, DatabaseEngine db) =>
        {
            try
            {
                var row = await db.GetAsync(tableName ?? table ?? TableNames.Default, key);
                return row is null ? Results.NotFound() : Results.Ok(row);
            }
            catch (TableNotFoundException ex)
            {
                return Results.NotFound(new { error = ex.Message });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        app.MapPut($"{routePrefix}/{{key}}", async (
            string? table,
            string key,
            [FromBody] PutValueRequest request,
            DatabaseEngine db,
            TableRaftRoleGuard tableRaft) =>
        {
            var targetTable = tableName ?? table ?? TableNames.Default;
            if (!tableRaft.CanAcceptWrites(targetTable))
            {
                return tableRaft.WriteRejectedResult(targetTable);
            }

            if (string.IsNullOrWhiteSpace(key))
            {
                return Results.BadRequest(new { error = "key is required" });
            }

            if (request.Value is null)
            {
                return Results.BadRequest(new { error = "value is required" });
            }

            try
            {
                await db.PutAsync(tableName ?? table ?? TableNames.Default, key, request.Value);
                return Results.NoContent();
            }
            catch (TableNotFoundException ex)
            {
                return Results.NotFound(new { error = ex.Message });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        app.MapDelete($"{routePrefix}/{{key}}", async (
            string? table,
            string key,
            DatabaseEngine db,
            TableRaftRoleGuard tableRaft) =>
        {
            var targetTable = tableName ?? table ?? TableNames.Default;
            if (!tableRaft.CanAcceptWrites(targetTable))
            {
                return tableRaft.WriteRejectedResult(targetTable);
            }

            if (string.IsNullOrWhiteSpace(key))
            {
                return Results.BadRequest(new { error = "key is required" });
            }

            try
            {
                await db.DeleteAsync(tableName ?? table ?? TableNames.Default, key);
                return Results.NoContent();
            }
            catch (TableNotFoundException ex)
            {
                return Results.NotFound(new { error = ex.Message });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });
    }
}

public sealed record PutValueRequest(string? Value);
public sealed record CreateViewRequest(string Query);

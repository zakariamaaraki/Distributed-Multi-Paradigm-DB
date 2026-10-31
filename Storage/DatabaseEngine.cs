using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using LsmWriteDb.ChangeLogs;
using LsmWriteDb.Indexes;
using LsmWriteDb.Search;

namespace LsmWriteDb.Storage;

public static class TableNames
{
    public const string Default = "kv";
    public const string Ownership = "__table_ownership";
    public static bool IsInternal(string table) => table.StartsWith("__", StringComparison.Ordinal);

    public static string Normalize(string table)
    {
        if (string.IsNullOrWhiteSpace(table))
        {
            throw new ArgumentException("Table name is required.", nameof(table));
        }

        var normalized = table.Trim().ToLowerInvariant();
        if (normalized.Length > 64)
        {
            throw new ArgumentException("Table name cannot be longer than 64 characters.", nameof(table));
        }

        if (!IsIdentifierStart(normalized[0]) || normalized.Any(character => !IsIdentifierPart(character)))
        {
            throw new ArgumentException("Table name must start with a letter or underscore and contain only letters, digits, and underscores.", nameof(table));
        }

        return normalized;
    }

    private static bool IsIdentifierStart(char character)
    {
        return character == '_' || character is >= 'a' and <= 'z';
    }

    private static bool IsIdentifierPart(char character)
    {
        return IsIdentifierStart(character) || character is >= '0' and <= '9';
    }
}

public sealed record TableInfo(string Name, string Kind = "table");

public sealed record TableStats(
    string Table,
    int MemTableEntries,
    int SstableCount,
    long LastSequence,
    int FlushThreshold,
    int BlockSizeBytes,
    long DiskSizeBytes);

public sealed record DatabaseStats(
    IReadOnlyList<TableStats> Tables,
    long LastSequence)
{
    private TableStats? DefaultTable => Tables.FirstOrDefault(table =>
        string.Equals(table.Table, TableNames.Default, StringComparison.Ordinal));

    public int MemTableEntries => DefaultTable?.MemTableEntries ?? 0;

    public int SstableCount => DefaultTable?.SstableCount ?? 0;

    public int FlushThreshold => DefaultTable?.FlushThreshold ?? 0;

    public int BlockSizeBytes => DefaultTable?.BlockSizeBytes ?? 0;
}

public sealed class TableNotFoundException : Exception
{
    public TableNotFoundException(string table)
        : base($"table '{table}' not found")
    {
        Table = table;
    }

    public string Table { get; }
}

public sealed class DatabaseEngine
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };

    private readonly LsmStoreOptions _options;
    private readonly ChangeLogService _changeLog;
    private readonly DatabaseSequenceGenerator _sequenceGenerator = new();
    private readonly ConcurrentDictionary<string, LsmStore> _stores = new(StringComparer.Ordinal);
    private readonly Dictionary<string, RelationalTableSchema> _relationalSchemas = new(StringComparer.Ordinal);
    private readonly Dictionary<string, VectorTableSchema> _vectorSchemas = new(StringComparer.Ordinal);
    private readonly Dictionary<string, LsmWriteDb.Indexes.HnswIndex> _vectorIndexes = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ViewDefinition> _views = new(StringComparer.Ordinal);
    private readonly JsonValueIndexStore _indexes;
    private readonly SstableValueIndexStore _sstableIndexes;
    private readonly FullTextSearchStore _search;
    private readonly SemaphoreSlim _catalogMutex = new(1, 1);
    private readonly string _tablesPath;
    private readonly string _catalogPath;

    private bool _initialized;

    public DatabaseEngine(LsmStoreOptions options, ChangeLogService changeLog)
    {
        _options = options;
        _changeLog = changeLog;
        _tablesPath = Path.Combine(options.DataPath, "tables");
        _catalogPath = Path.Combine(options.DataPath, "catalog.json");
        _indexes = new JsonValueIndexStore(options.DataPath);
        _sstableIndexes = new SstableValueIndexStore(options);
        _search = new FullTextSearchStore(options);
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await _catalogMutex.WaitAsync(cancellationToken);
        try
        {
            if (_initialized)
            {
                return;
            }

            Directory.CreateDirectory(_options.DataPath);
            Directory.CreateDirectory(_tablesPath);

            var catalog = await ReadCatalogAsync(cancellationToken);
            var tableNames = catalog.Tables.Select(TableNames.Normalize).ToHashSet(StringComparer.Ordinal);
            foreach (var schema in catalog.RelationalTables ?? [])
                _relationalSchemas[TableNames.Normalize(schema.Table)] = schema with { Table = TableNames.Normalize(schema.Table) };
            foreach (var schema in catalog.VectorTables ?? [])
                _vectorSchemas[TableNames.Normalize(schema.Table)] = schema with { Table = TableNames.Normalize(schema.Table) };
            foreach (var view in catalog.Views ?? [])
            {
                var normalizedView = TableNames.Normalize(view.Name);
                _views[normalizedView] = view with { Name = normalizedView, Kind = "view" };
            }
            tableNames.RemoveWhere(name => _views.ContainsKey(name));
            tableNames.Add(TableNames.Default);
            tableNames.Add(TableNames.Ownership);

            foreach (var directory in Directory.EnumerateDirectories(_tablesPath))
            {
                tableNames.Add(Path.GetFileName(directory));
            }

            foreach (var table in tableNames.OrderBy(name => name, StringComparer.Ordinal))
            {
                var store = GetOrCreateStoreCore(table);
                await store.InitializeAsync();
            }

            foreach (var schema in _vectorSchemas.Values)
            {
                schema.ValidateDefinition();
                var index = new LsmWriteDb.Indexes.HnswIndex(schema.Dimensions);
                foreach (var row in await ScanTableRowsForIndexAsync(schema.Table))
                    index.Upsert(row.Key, schema.ReadVector(row.Value));
                _vectorIndexes[schema.Table] = index;
            }

            await WriteCatalogAsync(_stores.Keys.OrderBy(name => name, StringComparer.Ordinal).ToList(), cancellationToken);
            await _indexes.InitializeAsync(
                _stores.Keys.OrderBy(name => name, StringComparer.Ordinal).ToList(),
                ScanTableRowsForIndexAsync,
                cancellationToken);

            await _search.InitializeAsync(_stores.Keys.ToHashSet(StringComparer.Ordinal), ScanTableRowsForIndexAsync, cancellationToken);
            await _sstableIndexes.InitializeAsync(_stores.Keys.OrderBy(name => name, StringComparer.Ordinal).ToList(), ScanTableRowsForIndexAsync, cancellationToken);
            _initialized = true;
        }
        finally
        {
            _catalogMutex.Release();
        }
    }

    public async Task<IReadOnlyList<TableInfo>> ListTablesAsync(CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        return _stores.Keys
            .Where(name => !TableNames.IsInternal(name))
            .Select(name => new TableInfo(
                name,
                string.Equals(name, TableNames.Default, StringComparison.Ordinal)
                    ? "kv"
                    : _vectorSchemas.ContainsKey(name) ? "vector" : _relationalSchemas.ContainsKey(name) ? "relational" : "document"))
            .Concat(_views.Values.Where(view => !TableNames.IsInternal(view.Name)).Select(view => new TableInfo(view.Name, "view")))
            .OrderBy(table => table.Name, StringComparer.Ordinal)
            .ToList();
    }

    internal async Task<IReadOnlyList<TableInfo>> ListAllTablesAsync(CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return _stores.Keys.OrderBy(name => name, StringComparer.Ordinal)
            .Select(name => new TableInfo(
                name,
                string.Equals(name, TableNames.Default, StringComparison.Ordinal)
                    ? "kv"
                : _vectorSchemas.ContainsKey(name) ? "vector" : _relationalSchemas.ContainsKey(name) ? "relational" : "document")).ToList();
    }
    public async Task<bool> CreateTableAsync(string table, CancellationToken cancellationToken = default)
    {
        return await CreateTableCoreAsync(table, schema: null, cancellationToken);
    }

    public async Task<bool> CreateRelationalTableAsync(RelationalTableSchema schema, CancellationToken cancellationToken = default)
    {
        schema.ValidateDefinition();
        var normalized = TableNames.Normalize(schema.Table);
        return await CreateTableCoreAsync(normalized, schema with { Table = normalized }, cancellationToken);
    }

    public async Task<bool> CreateVectorTableAsync(VectorTableSchema schema, CancellationToken cancellationToken = default)
    {
        schema.ValidateDefinition();
        var normalized = TableNames.Normalize(schema.Table);
        await EnsureInitializedAsync(cancellationToken);
        await _catalogMutex.WaitAsync(cancellationToken);
        try
        {
            if (_stores.ContainsKey(normalized)) return false;
            var store = GetOrCreateStoreCore(normalized);
            await store.InitializeAsync();
            _vectorSchemas[normalized] = schema with { Table = normalized };
            _vectorIndexes[normalized] = new LsmWriteDb.Indexes.HnswIndex(schema.Dimensions);
            await WriteCatalogAsync(_stores.Keys.OrderBy(name => name, StringComparer.Ordinal).ToList(), cancellationToken);
            return true;
        }
        finally { _catalogMutex.Release(); }
    }

    public async Task<bool> DropTableAsync(string table, CancellationToken cancellationToken = default)
    {
        var normalized = TableNames.Normalize(table);
        if (TableNames.IsInternal(normalized) || string.Equals(normalized, TableNames.Default, StringComparison.Ordinal))
            throw new ArgumentException("The default and internal tables cannot be dropped.", nameof(table));

        await EnsureInitializedAsync(cancellationToken);
        await _catalogMutex.WaitAsync(cancellationToken);
        try
        {
            if (_views.ContainsKey(normalized))
                throw new ArgumentException($"'{normalized}' is a view; DROP TABLE only accepts physical tables.", nameof(table));
            if (!_stores.TryRemove(normalized, out var store))
                return false;

            try
            {
                await store.DeleteDataAsync();
                _relationalSchemas.Remove(normalized);
                _vectorSchemas.Remove(normalized);
                _vectorIndexes.Remove(normalized);
                await _indexes.RemoveTableAsync(normalized, cancellationToken);
                await _search.RemoveTableAsync(normalized, cancellationToken);
                await _sstableIndexes.RemoveTableAsync(normalized, cancellationToken);
                await WriteCatalogAsync(_stores.Keys.OrderBy(name => name, StringComparer.Ordinal).ToList(), cancellationToken);
                return true;
            }
            catch
            {
                _stores[normalized] = store;
                throw;
            }
        }
        finally
        {
            _catalogMutex.Release();
        }
    }
    public async Task<ViewDefinition?> GetViewAsync(string view, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        _views.TryGetValue(TableNames.Normalize(view), out var definition);
        return definition;
    }

    public async Task<bool> CreateViewAsync(string view, string query, CancellationToken cancellationToken = default)
    {
        var normalized = TableNames.Normalize(view);
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("View query is required.", nameof(query));

        await EnsureInitializedAsync(cancellationToken);
        await _catalogMutex.WaitAsync(cancellationToken);
        try
        {
            if (_stores.ContainsKey(normalized) || _views.ContainsKey(normalized))
                return false;

            _views[normalized] = new ViewDefinition(normalized, query.Trim(), "view");
            await WriteCatalogAsync(_stores.Keys.OrderBy(name => name, StringComparer.Ordinal).ToList(), cancellationToken);
            return true;
        }
        finally
        {
            _catalogMutex.Release();
        }
    }
    public async Task<RelationalTableSchema?> GetRelationalSchemaAsync(string table, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        _relationalSchemas.TryGetValue(TableNames.Normalize(table), out var schema);
        return schema;
    }

    public async Task<VectorTableSchema?> GetVectorSchemaAsync(string table, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        _vectorSchemas.TryGetValue(TableNames.Normalize(table), out var schema);
        return schema;
    }

    public async Task<IReadOnlyList<(string Id, double Distance)>> SearchVectorsAsync(string table, IReadOnlyList<double> query, int k, int efSearch = 64)
    {
        await EnsureInitializedAsync();
        var normalized = TableNames.Normalize(table);
        if (!_vectorIndexes.TryGetValue(normalized, out var index)) throw new ArgumentException($"Table '{normalized}' is not a vector table.");
        return index.Search(query, k, efSearch);
    }

    public async Task ValidateWriteAsync(string table, string key, string value, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        if (_relationalSchemas.TryGetValue(TableNames.Normalize(table), out var schema))
            schema.ValidateRow(key, value);
        if (_vectorSchemas.TryGetValue(TableNames.Normalize(table), out var vectorSchema))
            vectorSchema.ReadVector(value);
    }

    private async Task<bool> CreateTableCoreAsync(string table, RelationalTableSchema? schema, CancellationToken cancellationToken)
    {
        var normalized = TableNames.Normalize(table);
        await EnsureInitializedAsync(cancellationToken);

        await _catalogMutex.WaitAsync(cancellationToken);
        try
        {
            if (_stores.ContainsKey(normalized))
                return false;

            var store = GetOrCreateStoreCore(normalized);
            await store.InitializeAsync();
            if (schema is not null)
                _relationalSchemas[normalized] = schema;
            await WriteCatalogAsync(_stores.Keys.OrderBy(name => name, StringComparer.Ordinal).ToList(), cancellationToken);
            return true;
        }
        finally
        {
            _catalogMutex.Release();
        }
    }

    public async Task<IReadOnlyList<JsonValueIndexInfo>> ListIndexesAsync(CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return await _indexes.ListAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<JsonValueIndexTreeDump>> DumpIndexTreesAsync(CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return await _indexes.DumpTreesAsync(cancellationToken);
    }

    public async Task<JsonValueIndexTreeDump?> DumpIndexTreeAsync(string name, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return await _indexes.DumpTreeAsync(name, cancellationToken);
    }

    public async Task<bool> CreateJsonValueIndexAsync(
        string table,
        string name,
        IReadOnlyList<string> path,
        CancellationToken cancellationToken = default)
    {
        var normalizedTable = TableNames.Normalize(table);
        var definition = new JsonValueIndexDefinition(
            IndexNames.Normalize(name),
            normalizedTable,
            path.Select(IndexNames.ValidatePathPart).ToList());

        await EnsureInitializedAsync(cancellationToken);
        var store = await GetStoreAsync(normalizedTable);
        var rows = await store.ScanAsync();

        return await _indexes.CreateAsync(definition, rows, cancellationToken);
    }

    public async Task<IReadOnlyList<string>?> TrySearchJsonValueIndexAsync(
        string table,
        IReadOnlyList<string> path,
        string expected,
        CancellationToken cancellationToken = default)
    {
        var normalizedTable = TableNames.Normalize(table);
        await GetStoreAsync(normalizedTable);
        return await _indexes.TrySearchAsync(normalizedTable, path, expected, cancellationToken);
    }

    public async Task<IReadOnlyList<SstableValueIndexInfo>> ListSstableValueIndexesAsync(CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return _sstableIndexes.List();
    }

    public async Task<bool> CreateSstableValueIndexAsync(string table, string name, IReadOnlyList<string> path, CancellationToken cancellationToken = default)
    {
        var normalizedTable = TableNames.Normalize(table);
        await GetStoreAsync(normalizedTable);
        return await _sstableIndexes.CreateAsync(normalizedTable, name, path, ScanTableRowsForIndexAsync, cancellationToken);
    }

    public async Task<IReadOnlyList<string>?> TrySearchSstableValueIndexAsync(string table, IReadOnlyList<string> path, string expected, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        await GetStoreAsync(table);
        return await _sstableIndexes.TrySearchAsync(table, path, expected, cancellationToken);
    }
    public async Task<IReadOnlyList<SearchIndexDefinition>> ListSearchIndexesAsync(CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return _search.List();
    }

    public async Task<bool> CreateSearchIndexAsync(string table, string name, IReadOnlyList<string> fields, CancellationToken cancellationToken = default)
    {
        var normalizedTable = TableNames.Normalize(table);
        await GetStoreAsync(normalizedTable);
        return await _search.CreateAsync(normalizedTable, name, fields, ScanTableRowsForIndexAsync, cancellationToken);
    }

    public async Task RebuildSearchIndexAsync(string name, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _search.RebuildAsync(name, ScanTableRowsForIndexAsync, cancellationToken);
    }

    public async Task<SearchResponse> SearchAsync(string name, SearchRequest request, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return await _search.SearchAsync(name, request, cancellationToken);
    }
    public async Task PutAsync(string table, string key, string value)
    {
        await ValidateWriteAsync(table, key, value);
        var store = await GetStoreAsync(table);
        var oldRow = await store.GetAsync(key);
        await store.PutAsync(key, value);
        ApplyVectorPut(TableNames.Normalize(table), key, value);
        await _indexes.ApplyPutAsync(TableNames.Normalize(table), key, oldRow?.Value, value);
        await _search.ApplyPutAsync(TableNames.Normalize(table), key, oldRow?.Value, value);
        await _sstableIndexes.ApplyPutAsync(TableNames.Normalize(table), key, oldRow?.Value, value);
    }

    public async Task DeleteAsync(string table, string key)
    {
        var store = await GetStoreAsync(table);
        var oldRow = await store.GetAsync(key);
        await store.DeleteAsync(key);
        if (_vectorIndexes.TryGetValue(TableNames.Normalize(table), out var vectorIndex)) vectorIndex.Remove(key);
        await _indexes.ApplyDeleteAsync(TableNames.Normalize(table), key, oldRow?.Value);
        await _search.ApplyDeleteAsync(TableNames.Normalize(table), key, oldRow?.Value);
        await _sstableIndexes.ApplyDeleteAsync(TableNames.Normalize(table), key, oldRow?.Value);
    }

    public async Task<KeyValueRow?> GetAsync(string table, string key)
    {
        var store = await GetStoreAsync(table);
        return await store.GetAsync(key);
    }

    public async Task<TableSnapshot> GetSnapshotAsync(string table)
    {
        var normalized = TableNames.Normalize(table);
        var store = await GetStoreAsync(normalized);
        var rows = await store.ScanAsync();
        var stats = await store.GetStatsAsync();
        return new TableSnapshot(normalized, stats.LastSequence, rows);
    }
    public async Task<IReadOnlyList<KeyValueRow>> RangeAsync(string table, string? start, string? end, int limit)
    {
        var store = await GetStoreAsync(table);
        return await store.RangeAsync(start, end, limit);
    }

    public async Task ApplyBatchAsync(IReadOnlyList<StoreWriteOperation> operations)
    {
        if (operations.Count == 0)
        {
            return;
        }

        await EnsureInitializedAsync();

        foreach (var operation in operations.Where(operation => !operation.IsDeleted))
            await ValidateWriteAsync(operation.Table, operation.Key, operation.Value ?? string.Empty);

        foreach (var group in operations.GroupBy(operation => TableNames.Normalize(operation.Table)))
        {
            var store = await GetStoreAsync(group.Key);
            var groupOperations = group.ToList();
            var oldRowsByKey = new Dictionary<string, string?>(StringComparer.Ordinal);
            foreach (var operation in groupOperations)
            {
                oldRowsByKey[operation.Key] = (await store.GetAsync(operation.Key))?.Value;
            }

            await store.ApplyBatchAsync(groupOperations);

            foreach (var operation in groupOperations)
            {
                if (operation.IsDeleted)
                {
                    if (_vectorIndexes.TryGetValue(group.Key, out var vectorIndex)) vectorIndex.Remove(operation.Key);
                    await _indexes.ApplyDeleteAsync(group.Key, operation.Key, oldRowsByKey[operation.Key]);
                    await _search.ApplyDeleteAsync(group.Key, operation.Key, oldRowsByKey[operation.Key]);
                    await _sstableIndexes.ApplyDeleteAsync(group.Key, operation.Key, oldRowsByKey[operation.Key]);
                }
                else
                {
                    ApplyVectorPut(group.Key, operation.Key, operation.Value ?? string.Empty);
                    await _indexes.ApplyPutAsync(group.Key, operation.Key, oldRowsByKey[operation.Key], operation.Value ?? string.Empty);
                    await _search.ApplyPutAsync(group.Key, operation.Key, oldRowsByKey[operation.Key], operation.Value ?? string.Empty);
                    await _sstableIndexes.ApplyPutAsync(group.Key, operation.Key, oldRowsByKey[operation.Key], operation.Value ?? string.Empty);
                }
            }
        }
    }

    public async Task ApplyReplicatedChangeAsync(ChangeLogEntry entry)
    {
        await EnsureInitializedAsync();

        var table = TableNames.Normalize(entry.Table);
        if (!entry.IsDeleted)
            await ValidateWriteAsync(table, entry.Key, entry.Value ?? string.Empty);
        if (!_stores.ContainsKey(table))
        {
            await CreateTableAsync(table);
        }

        var store = await GetStoreAsync(table);
        var oldRow = await store.GetAsync(entry.Key);
        await store.ApplyReplicatedChangeAsync(entry);

        if (entry.IsDeleted)
        {
            if (_vectorIndexes.TryGetValue(table, out var vectorIndex)) vectorIndex.Remove(entry.Key);
            await _indexes.ApplyDeleteAsync(table, entry.Key, oldRow?.Value);
            await _search.ApplyDeleteAsync(table, entry.Key, oldRow?.Value);
            await _sstableIndexes.ApplyDeleteAsync(table, entry.Key, oldRow?.Value);
        }
        else
        {
            ApplyVectorPut(table, entry.Key, entry.Value ?? string.Empty);
            await _indexes.ApplyPutAsync(table, entry.Key, oldRow?.Value, entry.Value ?? string.Empty);
            await _search.ApplyPutAsync(table, entry.Key, oldRow?.Value, entry.Value ?? string.Empty);
            await _sstableIndexes.ApplyPutAsync(table, entry.Key, oldRow?.Value, entry.Value ?? string.Empty);
        }
    }

    public async Task<DatabaseStats> GetStatsAsync()
    {
        await EnsureInitializedAsync();

        var stats = new List<TableStats>();
        foreach (var table in _stores.Keys.Where(name => !TableNames.IsInternal(name)).OrderBy(name => name, StringComparer.Ordinal))
        {
            stats.Add(await GetTableStatsAsync(table));
        }

        return new DatabaseStats(stats, _sequenceGenerator.LastSequence);
    }

    public long GetTotalDiskSizeBytes()
    {
        return GetDirectorySize(_options.DataPath);
    }
    public async Task<TableStats> GetTableStatsAsync(string table)
    {
        var normalized = TableNames.Normalize(table);
        var store = await GetStoreAsync(normalized);
        var stats = await store.GetStatsAsync();
        return new TableStats(
            normalized,
            stats.MemTableEntries,
            stats.SstableCount,
            stats.LastSequence,
            stats.FlushThreshold,
            stats.BlockSizeBytes,
            GetDirectorySize(Path.Combine(_tablesPath, normalized)));
    }

    private async Task<LsmStore> GetStoreAsync(string table)
    {
        var normalized = TableNames.Normalize(table);
        await EnsureInitializedAsync();

        if (_stores.TryGetValue(normalized, out var store))
        {
            return store;
        }

        throw new TableNotFoundException(normalized);
    }

    private static long GetDirectorySize(string path)
    {
        if (!Directory.Exists(path))
            return 0;

        long total = 0;
        foreach (var file in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
        {
            try { total += new FileInfo(file).Length; }
            catch (FileNotFoundException) { }
            catch (DirectoryNotFoundException) { }
        }
        return total;
    }
    private async Task<IReadOnlyList<KeyValueRow>> ScanTableRowsForIndexAsync(string table)
    {
        if (_stores.TryGetValue(TableNames.Normalize(table), out var store))
        {
            return await store.ScanAsync();
        }

        throw new TableNotFoundException(table);
    }

    private void ApplyVectorPut(string table, string key, string value)
    {
        if (_vectorSchemas.TryGetValue(table, out var schema) && _vectorIndexes.TryGetValue(table, out var index))
            index.Upsert(key, schema.ReadVector(value));
    }

    private LsmStore GetOrCreateStoreCore(string table)
    {
        var normalized = TableNames.Normalize(table);
        return _stores.GetOrAdd(normalized, name =>
        {
            var tablePath = Path.Combine(_tablesPath, name);
            return new LsmStore(
                new LsmStoreOptions(
                    tablePath,
                    _options.FlushThreshold,
                    name,
                    _options.BlockSizeBytes,
                    _options.MaxSstableFileSizeBytes),
                _changeLog,
                _sequenceGenerator);
        });
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized)
        {
            return;
        }

        await InitializeAsync(cancellationToken);
    }

    private async Task<TableCatalogSnapshot> ReadCatalogAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_catalogPath))
            return new TableCatalogSnapshot([]);

        await using var stream = new FileStream(_catalogPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        return await JsonSerializer.DeserializeAsync<TableCatalogSnapshot>(stream, JsonOptions, cancellationToken)
            ?? new TableCatalogSnapshot([]);
    }

    private async Task WriteCatalogAsync(IReadOnlyList<string> tables, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(_options.DataPath);
        var snapshot = new TableCatalogSnapshot(tables, _relationalSchemas.Values.OrderBy(schema => schema.Table, StringComparer.Ordinal).ToList(), _views.Values.OrderBy(view => view.Name, StringComparer.Ordinal).ToList(), _vectorSchemas.Values.OrderBy(schema => schema.Table, StringComparer.Ordinal).ToList());
        var json = JsonSerializer.Serialize(snapshot, JsonOptions);
        await File.WriteAllTextAsync(_catalogPath, json, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), cancellationToken);
    }
}

public interface IStoreSequenceGenerator
{
    long NextSequence();

    void Observe(long sequence);
}

internal sealed class DatabaseSequenceGenerator : IStoreSequenceGenerator
{
    private readonly object _mutex = new();
    private long _lastSequence;

    public long LastSequence
    {
        get
        {
            lock (_mutex)
            {
                return _lastSequence;
            }
        }
    }

    public long NextSequence()
    {
        lock (_mutex)
        {
            return ++_lastSequence;
        }
    }

    public void Observe(long sequence)
    {
        lock (_mutex)
        {
            _lastSequence = Math.Max(_lastSequence, sequence);
        }
    }
}

public sealed record ViewDefinition(string Name, string Query, string Kind = "view");

internal sealed record TableCatalogSnapshot(IReadOnlyList<string> Tables, IReadOnlyList<RelationalTableSchema>? RelationalTables = null, IReadOnlyList<ViewDefinition>? Views = null, IReadOnlyList<VectorTableSchema>? VectorTables = null);

namespace LsmWriteDb.Indexes;

/// <summary>A small in-memory HNSW index. Source vectors remain in the LSM table.</summary>
public sealed class HnswIndex
{
    private readonly int _dimensions;
    private readonly int _m;
    private readonly int _efConstruction;
    private readonly Random _random;
    private readonly Dictionary<string, Node> _nodes = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private Node? _entry;
    private int _maxLevel = -1;

    public HnswIndex(int dimensions, int m = 16, int efConstruction = 200, int? randomSeed = null)
    {
        if (dimensions < 1) throw new ArgumentOutOfRangeException(nameof(dimensions));
        if (m < 2) throw new ArgumentOutOfRangeException(nameof(m));
        if (efConstruction < m) throw new ArgumentOutOfRangeException(nameof(efConstruction));
        _dimensions = dimensions;
        _m = m;
        _efConstruction = efConstruction;
        _random = randomSeed is int seed ? new Random(seed) : Random.Shared;
    }

    public int Count { get { lock (_gate) return _nodes.Values.Count(node => !node.Deleted); } }

    public void Upsert(string id, IReadOnlyList<double> vector)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ValidateVector(vector);
        lock (_gate)
        {
            if (_nodes.TryGetValue(id, out var previous)) previous.Deleted = true;
            var level = RandomLevel();
            var node = new Node(id, vector.ToArray(), level);
            _nodes[id] = node;
            if (_entry is null)
            {
                _entry = node;
                _maxLevel = level;
                return;
            }

            var current = _entry;
            for (var layer = _maxLevel; layer > level; layer--)
                current = GreedySearch(vector, current, layer);

            for (var layer = Math.Min(level, _maxLevel); layer >= 0; layer--)
            {
                var candidates = SearchLayer(vector, current, _efConstruction, layer);
                var selected = candidates.OrderBy(item => Distance(vector, item.Vector)).Take(_m).ToList();
                node.Neighbors[layer].AddRange(selected);
                foreach (var neighbor in selected)
                {
                    neighbor.Neighbors[layer].Add(node);
                    Prune(neighbor, layer);
                }
                if (candidates.Count > 0) current = candidates.OrderBy(item => Distance(vector, item.Vector)).First();
            }

            if (level > _maxLevel)
            {
                _entry = node;
                _maxLevel = level;
            }
        }
    }

    public bool Remove(string id)
    {
        lock (_gate)
        {
            if (!_nodes.TryGetValue(id, out var node) || node.Deleted) return false;
            node.Deleted = true;
            return true;
        }
    }

    public IReadOnlyList<(string Id, double Distance)> Search(IReadOnlyList<double> query, int k, int efSearch = 64)
    {
        ValidateVector(query);
        if (k < 1) throw new ArgumentOutOfRangeException(nameof(k));
        if (efSearch < k) efSearch = k;
        lock (_gate)
        {
            if (_entry is null) return [];
            var current = _entry;
            for (var layer = _maxLevel; layer > 0; layer--) current = GreedySearch(query, current, layer);
            var candidates = SearchLayer(query, current, efSearch, 0);
            return candidates.Where(node => !node.Deleted)
                .Select(node => (node.Id, Distance(query, node.Vector)))
                .OrderBy(item => item.Item2)
                .Take(k)
                .ToList();
        }
    }

    private List<Node> SearchLayer(IReadOnlyList<double> query, Node entry, int ef, int layer)
    {
        var visited = new HashSet<Node> { entry };
        var candidates = new PriorityQueue<Node, double>();
        var results = new PriorityQueue<Node, double>();
        var entryDistance = Distance(query, entry.Vector);
        candidates.Enqueue(entry, entryDistance);
        results.Enqueue(entry, -entryDistance);
        while (candidates.TryDequeue(out var candidate, out var distance))
        {
            results.TryPeek(out _, out var worstPriority);
            if (results.Count >= ef && distance > -worstPriority) break;
            if (candidate.Neighbors.Length <= layer) continue;
            foreach (var neighbor in candidate.Neighbors[layer])
            {
                if (!visited.Add(neighbor)) continue;
                var neighborDistance = Distance(query, neighbor.Vector);
                results.TryPeek(out _, out worstPriority);
                if (results.Count < ef || neighborDistance < -worstPriority)
                {
                    candidates.Enqueue(neighbor, neighborDistance);
                    results.Enqueue(neighbor, -neighborDistance);
                    if (results.Count > ef) results.Dequeue();
                }
            }
        }
        return results.UnorderedItems.Select(item => item.Element).ToList();
    }

    private Node GreedySearch(IReadOnlyList<double> query, Node entry, int layer)
    {
        var current = entry;
        var changed = true;
        while (changed)
        {
            changed = false;
            if (current.Neighbors.Length <= layer) return current;
            var distance = Distance(query, current.Vector);
            foreach (var neighbor in current.Neighbors[layer])
            {
                var candidateDistance = Distance(query, neighbor.Vector);
                if (candidateDistance >= distance) continue;
                current = neighbor;
                distance = candidateDistance;
                changed = true;
            }
        }
        return current;
    }

    private void Prune(Node node, int layer)
    {
        var neighbors = node.Neighbors[layer];
        if (neighbors.Count <= _m * 2) return;
        var retained = neighbors.OrderBy(other => Distance(node.Vector, other.Vector)).Take(_m).ToList();
        neighbors.Clear();
        neighbors.AddRange(retained);
    }

    private int RandomLevel()
    {
        var level = 0;
        var scale = 1.0 / Math.Log(_m);
        while (_random.NextDouble() < Math.Exp(-1.0 / scale)) level++;
        return level;
    }

    private void ValidateVector(IReadOnlyList<double> vector)
    {
        ArgumentNullException.ThrowIfNull(vector);
        if (vector.Count != _dimensions) throw new ArgumentException($"Expected a vector with {_dimensions} dimensions.", nameof(vector));
        if (vector.Any(value => !double.IsFinite(value))) throw new ArgumentException("Vector components must be finite numbers.", nameof(vector));
    }

    private static double Distance(IReadOnlyList<double> left, IReadOnlyList<double> right)
    {
        double dot = 0, leftNorm = 0, rightNorm = 0;
        for (var index = 0; index < left.Count; index++)
        {
            dot += left[index] * right[index];
            leftNorm += left[index] * left[index];
            rightNorm += right[index] * right[index];
        }
        if (leftNorm == 0 || rightNorm == 0) return leftNorm == rightNorm ? 0 : 1;
        return 1 - dot / Math.Sqrt(leftNorm * rightNorm);
    }

    private sealed class Node(string id, double[] vector, int level)
    {
        public string Id { get; } = id;
        public double[] Vector { get; } = vector;
        public bool Deleted { get; set; }
        public List<Node>[] Neighbors { get; } = Enumerable.Range(0, level + 1).Select(_ => new List<Node>()).ToArray();
    }
}

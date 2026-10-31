# Vector Tables and HNSW Index

## Decision

Add vector tables as a fourth logical paradigm over the existing per-table LSM
engine. Rows retain the normal string key and JSON value representation; the
embedding is a validated JSON numeric array. A table declares one fixed vector
dimension. The initial implementation uses cosine distance; additional metrics
can be added to schema metadata after distance semantics are consistent across
index rebuilds.

```sql
CREATE TABLE VECTOR documents (embedding VECTOR(768))
```

The HNSW graph is a derived index. The LSM row is authoritative. The graph is
rebuilt from durable table rows at startup and updated after local writes,
transaction commits, and replicated change application. A crash between row
commit and graph update is repaired by rebuilding on restart. Vector search is
eventually consistent with writes on a follower, just like other follower
reads; strong search routes to the table leader. The first version is
single-process per table replica and does not claim a cluster-wide graph.

The SQL surface should expose vector nearest-neighbor search explicitly:

```sql
SELECT key, value FROM documents
ORDER BY embedding <-> '[0.1, 0.2, ...]'
LIMIT 10
```

The parser validates query dimensions and `LIMIT`; the execution result returns
the existing row payload and its distance. Metadata predicates can be applied
to returned candidates in a later extension. `CREATE TABLE VECTOR` schemas
are cataloged separately from relational schemas and replicated with table
creation. Vector dimensions are immutable for the first release.

## Index lifecycle and correctness

- Validate finite numeric components and exact dimension before the write is
  acknowledged, including staged transaction writes and replicated writes.
- Use the row key as the stable HNSW node identity. Replacing a key removes the
  previous node logically, then inserts the replacement. Deletion marks a node
  deleted; periodic graph rebuild reclaims tombstones.
- Keep graph mutation synchronized per table. Inserts use HNSW's randomized
  level assignment and greedy descent; search uses a min-heap of candidates and
  a bounded result heap. `efSearch` controls recall/latency and must be at least
  `k`.
- Persist only table vector schema as catalog metadata initially. On startup,
  rebuild the derived graph by scanning rows. A later optimization may add a
  versioned graph snapshot plus WAL/change sequence, but recovery must always
  fall back to rebuilding from LSM rows.
- Default parameters: `M=16`, `efConstruction=200`, `efSearch=64`; expose
  bounded tuning only after benchmarks. Search reports approximate results.
- Reject regular relational/document index creation on the embedding field and
  reject malformed vectors with a useful SQL error.

## Algorithm comparison

| Approach | Search behavior | Write/update cost | Memory/disk profile | Fit here |
|---|---|---|---|---|
| Exact KNN / flat scan | Exact, O(N·d) distance work | Excellent | Minimal index; scans LSM rows | Best for small tables and exact baseline, but latency scales linearly |
| HNSW | Approximate graph traversal; high recall and low latency in practice | Incremental inserts; deletes need tombstones/rebuild | Memory-heavy graph | Chosen: dynamic writes suit the database and graph is straightforward to rebuild from LSM |
| IVF-Flat | Approximate search over selected centroids; tunable probes | Centroid training and list maintenance | Moderate; vectors remain uncompressed | Good alternative for larger disk-oriented collections; needs training/retraining lifecycle |
| IVF-PQ | Approximate compressed search | Training and codebook maintenance | Low memory per vector | Useful when RAM is the constraint; reduced distance fidelity and more operational machinery |
| Random projection / LSH | Approximate hash-bucket candidates | Easy incremental updates | Multiple hash tables; recall can require many probes | Better for particular metrics/distributions; less reliable latency/recall tradeoff |
| Annoy / random projection trees | Approximate forest traversal | Primarily build-once, updates often require rebuild | Memory mapped, read optimized | Poor fit for a write-heavy workload |
| DiskANN / graph on SSD | Approximate graph traversal with lower RAM use | More complex graph maintenance and SSD I/O | Disk-oriented | Strong scale-out option, but significantly more implementation and tuning complexity |

KNN names the neighbor-ranking task; exact KNN is the flat scan above, while
ANN methods trade a controllable amount of recall for latency. HNSW is the
initial index, and exact scan remains useful as a correctness oracle and
small-table fallback. Before production parameter changes, compare recall@k,
p50/p95 latency, insert throughput, memory per vector, and restart rebuild time
against exact scan and at least IVF-Flat on representative embeddings.

## Rollout and limits

The initial version provides one embedding column and one graph per table. It
does not include metadata filtering inside graph traversal, quantization,
multi-vector rows, hybrid ranking, online schema migration, or a durable graph
snapshot. Catalog/schema replication and ordinary row replication remain the
existing table system's responsibility. Graph rebuild time and memory use are
the main limits; a benchmark should establish practical collection size before
deployment.

# Distributed Write-Heavy Database

A distributed, write-optimized database built around one durable LSM storage engine. It exposes four data paradigms over the same data: key/value, JSON documents, SQL-shaped tables, and vector tables. Exact-value indexes and full-text search are additional capabilities over tables.

The four supported data paradigms are:

- **Key/value store**: opaque string values addressed by a string key. Use it for simple high-throughput reads and writes when the application owns the value format.
- **Document database**: JSON documents addressed by a string key, with optional JSON property indexes and dot-path queries. Use it for flexible records whose shape can evolve without a fixed relational schema.
- **Relational database**: schema-defined SQL tables whose rows are encoded as JSON internally, with the primary key kept as the physical table key. Use it when typed columns, primary keys, validation, SQL projections, joins, and transactions matter.
- **Vector database**: fixed-dimension embeddings stored in JSON rows, with a derived HNSW index for approximate nearest-neighbor search. Use it for similarity search over embeddings; the graph is rebuilt from durable LSM rows at startup.

Additional supported search capabilities:

- **Exact-value indexes**: accelerate lookups over indexed values in existing tables.
- **Full-text search**: search text fields over ordinary tables with phrase matching, AND/OR queries, and relevance ranking. It indexes existing rows rather than introducing another row model.

The central design rule is simple:

> Keep the source table authoritative, make writes durable through the same WAL and SSTable pipeline, and build higher-level capabilities above that foundation.

## Why relational systems are harder to scale horizontally

Relational databases can scale, but they often scale less simply when horizontal distribution must preserve rich relational guarantees. The difficulty is not the existence of a schema alone; it is the coordination required by the operations the relational model makes possible. Horizontal scaling becomes harder when a workload needs:

- joins across partitions;
- foreign-key and uniqueness constraints across nodes;
- secondary indexes that span partitions;
- multi-row or multi-table transactions;
- serializable consistency and a single globally ordered view of data;
- write hotspots caused by popular rows or shared counters.

This project treats those tradeoffs as an architectural problem rather than assuming that SQL and scale are incompatible. We added one Raft leader per table so write ownership and ordering are local to a table, while different tables can have different leaders and scale independently. Cross-table writes use coordinator-driven two-phase commit only when coordination is actually required.

We also support multiple read consistency levels—eventual, session, bounded staleness, and strong—so every read does not pay the cost of leader routing when the use case does not need it. Transaction isolation is separately selectable: read committed for short independent operations, repeatable read for stable repeated reads, and serializable when serializable peers must not overlap.

The result is a layered design: the relational schema and SQL behavior remain available, while table-local leadership, selective consistency, per-table LSM storage, derived indexes, and explicit distributed coordination let deployments choose where to spend coordination and where to favor throughput or availability.

## What this database gives you

| Need | Capability | Storage model |
|---|---|---|
| Direct records | Key/value API | String key → opaque string value |
| Flexible JSON | Document API and dot-path predicates | String key → JSON string |
| Familiar queries | SQL and TCP SQL | `key` plus JSON-backed `value` |
| Exact secondary lookup | B+ tree or `USING FASTWRITE` | Derived `(value, rowKey)` entries |
| Text search | REST and SQL full-text search | Derived inverted-index postings |
| Distributed writes | Per-table Raft ownership | One leader and replicated followers per table |
| Cross-table changes | Local transactions and 2PC | Staged writes plus durable commit records |
| External consumers | Ordered change log and SSE | Replayable committed events |

## Start here

### Run it locally

```powershell
dotnet run --project LsmWriteDb.csproj
```

The HTTP service listens on port `8080` by default. The TCP SQL listener uses
`127.0.0.1:6543` when enabled. For a complete three-node deployment, use:

```bash
docker compose up --build
```

### First request

```powershell
curl.exe -X PUT http://localhost:8080/tables/users
curl.exe -X PUT http://localhost:8080/tables/users/kv/alice `
  -H "Content-Type: application/json" `
  -d '{"name":"Alice","tier":"gold"}'
curl.exe http://localhost:8080/tables/users/kv/alice
```

### First SQL query

```powershell
curl.exe -X POST http://localhost:8080/sql `
  -H "Content-Type: application/json" `
  -d '{"query":"SELECT * FROM users WHERE value.tier = ''gold''"}'
```

For Docker, clustered routing, transactions, the Python client, or TCP SQL,
jump to the corresponding section below.

## Reader map

1. [Goals and guarantees](#goals)
2. [Architecture](#architecture)
3. [Per-table leadership and Router](#per-table-leadership)
4. [Transactions and consistency](#distributed-transactions)
5. [Transaction isolation levels](#transaction-isolation-levels)
6. [Data model and SQL](#data-model)
7. [Indexes and full-text search](#b-trees-and-json-value-indexes)
8. [HTTP, Python, and TCP interfaces](#http-api)
9. [Storage internals](#components)
10. [Read/write paths and storage layout](#write-path)
11. [Testing and deployment](#testing)

## The one-minute mental model

```text
Client
  │ REST / SQL / TCP SQL / Python
  ▼
Router (optional) ── discovers the table leader
  ▼
Table role guard ── leader accepts writes; followers serve reads
  ▼
Source table LSM store
  ├─ WAL
  ├─ ordered memtable
  ├─ Tier 0: multiple immutable SSTable runs
  │    ├─ run A: sstable-001 + sstable-002
  │    └─ run B: sstable-003 + sstable-004
  ├─ four logical runs merge/promote to Tier 1
  │    ├─ Tier 1: multiple bounded SSTables
  │    └─ Tier 2: older promoted runs
  ├─ Bloom filters + sparse block indexes per SSTable
  └─ tiered compaction
       │
       ├─ B+ tree exact-value index
       ├─ FASTWRITE exact-value LSM index
       ├─ full-text inverted index
       └─ committed change log → follower replication / consumers
```

Source rows are authoritative. Indexes are derived structures that accelerate
specific reads and can be rebuilt from source data. Search postings and
`FASTWRITE` entries use the same LSM/SSTable machinery as ordinary tables.

## Architecture

### High-level architecture

![High-level per-table leadership architecture](./docs/high-level-architecture.svg?raw=true)

The cluster has independent leadership groups per table. A node may lead one
table while following another. The internal `__table_ownership` table records
which nodes currently own each table, the table term, leader URL, and rebalance
identifier.

### Low-level table flow

![Low-level table election, write, and bootstrap flow](./docs/low-level-table-architecture.svg?raw=true)

Writes first pass through the table-specific role guard. The table leader
appends to that table's WAL and memtable, then publishes a committed event.
Followers bootstrap with a table snapshot and continue from its sequence through
the SSE change stream. Each follower filters the shared change log by table.

Flushes and tiered compactions create multiple bounded immutable SSTable files grouped into logical runs.
Each file has its own Bloom filter and sparse-index sidecars, so a large sorted
run never requires one unbounded JSON data file. The configured maximum is a
storage safety limit; a single oversized record/block may exceed it because a
record is never split across blocks.
### Supported architecture

- Independent Raft terms, votes, heartbeats, and elections per table.
- Multiple followers per table and different leaders for different tables.
- Table-specific write rejection with the current leader URL.
- Runtime peer registration through `POST /raft/membership/register`.
- Health-based automatic and explicit rebalancing through `POST /raft/rebalance`.
- Internal `__table_ownership` metadata storage, hidden from public table lists.
- Follower snapshot bootstrap through `GET /tables/{table}/snapshot`.
- Per-table applied sequence tracking and SSE reconnection.
- Table-local WALs and shared database change-log events.
- New tables are initially placed on the healthy node with the fewest user tables.
- Table-catalog creation is propagated to every configured peer before writes are accepted.
- Distributed transaction coordination uses two-phase commit across the affected table leaders.
## Goals

- Optimize for frequent writes.
- Support point reads and range reads.
- Support four semantic read consistency levels: eventual, session (with monotonic and
  consistent-prefix aliases), bounded staleness, and strong.
- Route all transactional reads through the relevant table leader for read-your-writes,
  while non-transactional reads follow the selected consistency level.
- Support multiple logical tables, each with its own WAL, memtable, SSTables,
  Bloom filters, and compaction lifecycle.
- Use Bloom filters to skip SSTables that definitely do not contain a key.
- Use configurable SSTable blocks with sparse indexes so point reads can seek
  to a small block instead of reading a whole SSTable.
- Support explicit local transactions and distributed transactions across table leaders using two-phase commit.
- Coordinate multi-table commits with a coordinator-driven prepare/commit/abort protocol and explicit `in-doubt` outcomes.
- Bound each physical SSTable file so large flushes and compactions produce
  multiple manageable files instead of one unbounded file.
- Persist distributed transaction coordinator and participant state for restart recovery, idempotent phase retries, and automatic expiration cleanup.
- Expose distributed transaction status, recovery, structured logs, and phase metrics for operations.
- Use read-your-writes with selectable transaction isolation: read committed, repeatable read, and serializable. Repeatable read caches transaction reads; serializable uses a strict coarse-grained 2PL gate. Snapshot isolation remains unsupported.
- Keep uncommitted transaction changes out of durable storage after a server crash.
- Provide a modular SQL engine over the existing key/value and transaction APIs.
- Support four compatible data paradigms: key/value, JSON document, schema-enforced relational tables, and vector tables over the same storage engine.
- Support SQL table creation, point reads, key range reads, inserts, updates,
  deletes, transaction control, and equality filters over JSON `value`
  documents or dot-path properties.
- Enforce valid JSON documents for SQL `INSERT` and `UPDATE` values while
  keeping the lower-level key/value storage string-based.
- Support disk-backed B+ tree indexes over JSON `value` documents or dot-path
  properties for equality searches.
- Support `USING FASTWRITE` exact-value indexes backed by the normal LSM/SSTable
  write path for high-ingest workloads.
- Support inner equi-joins between tables on matching keys.
- Support read-only SQL views stored as catalog metadata over key/value, document, relational, and vector tables.
- Elect one leader per table with multiple followers and rebalance table ownership when replicas join or leave.
- Publish committed change-log events so replicas and external consumers can
  replay changes or subscribe over Server-Sent Events from a known sequence
  number.
- Run independent Raft elections per table, with one leader and multiple followers per table.
- Do not acknowledge table creation until the table has a discoverable elected leader.
- Flush in-memory data to disk when a size threshold is reached.
- Create a new tier-0 logical run after each flush and promote groups of four runs through higher tiers.
- Keep each logical run as multiple immutable SSTable files when size requires it, grouped by a shared run ID.
- Compact and promote logical runs by tier while preserving read correctness across all retained tiers.

## Per-Table Leadership

The cluster uses an independent Raft group for each logical table. A node can be
leader for `users` while following `orders`, allowing write load to be spread
across replicas. Each table leader accepts writes for that table; table
followers reject writes with the current table leader URL and replicate that
table's committed change stream.

Ownership records are stored in the internal `__table_ownership` table. The
record contains the table term, leader, members, and rebalance identifier. The
internal table is hidden from normal table listings and is bootstrapped with the
same table-leadership mechanism.

When a node joins or becomes unavailable, the ownership planner selects a new
member set and table leader. A table election requires a majority of that
table's members. A table without a majority becomes unavailable for writes to
avoid split-brain behavior. See [design/table-leadership.md](./design/table-leadership.md)
for the ownership, bootstrap, failure, and rebalancing protocol.

Distributed transactions use a coordinator-driven two-phase commit protocol across table leaders. Cross-table JOINs remain read-only.

### Table creation and load balancing

CREATE TABLE is cluster-wide. When the Router cannot discover an existing
leader for the requested table, it counts the user tables known by each healthy
node and sends the create request to the node with the fewest tables. That node
creates the local store and propagates the catalog entry to all configured
peers. Each peer initializes the table and its table-specific Raft state. Creation then waits
for a discoverable elected leader before returning success, so the table exists on
every replica and is ready for distributed writes.
Restarted followers also discover missing table names from peers and bootstrap their
local stores before serving eventual reads. If the bounded readiness timeout
expires, creation returns `503 Service Unavailable` and clients should retry.

If a peer is unavailable during creation, verify the table lists and monitoring
page before using the table in a distributed transaction. Clients should use a
Router URL rather than a database node URL.

## Router Process

Each database node can run with a companion `Router` process. The Router is an
HTTP reverse proxy that keeps pooled, long-lived connections to the database
nodes. Writes and transactional reads route to table leaders; ordinary reads are
load-balanced across healthy replicas unless the client requests session, bounded-staleness, or strong consistency.

The Router:

- Places new tables on the healthy node with the fewest user tables.

- Discovers a table leader through `/raft/tables/{table}/state`.
- Caches the leader URL per table to avoid discovery on every request.
- Forwards REST, SQL, and transaction requests while preserving method, headers,
  query string, and request body.
- Invalidates its cached leader after a leader-rejection or redirect and retries
  discovery once.
- Uses HTTP keep-alive/connection pooling and HTTP/2 connection support so the
  Router does not create a new TCP connection for every request.
- Falls back to its configured database node when no leader can be discovered.

Run it with:

```powershell
$env:ROUTER_DATABASE_URL = 'http://node-a:8080'
$env:ROUTER_PEERS = 'node-b=http://node-b:8080,node-c=http://node-c:8080'
dotnet run --project Router/Router.csproj --urls http://localhost:9080
```

### Read routing and consistency

The Router supports four semantic consistency levels for ordinary point and range reads: eventual, session, bounded staleness, and strong. `monotonic` and `consistent-prefix` are aliases for session. The
default is eventual consistency: when `X-Read-Consistency` is absent or set to
`eventual`, the Router load-balances the read across healthy replicas. A follower
may be briefly behind while it catches up from the leader's committed change
stream.

Send `X-Read-Consistency: strong` when the read must go through the current table
leader and return the leader's latest committed state. If the leader cannot be
discovered, the strong read fails instead of falling back to a follower.

### Choosing strong, eventual, bounded-staleness, or session reads

Choose **strong consistency** when the caller must immediately observe the latest committed write. Strong reads are routed to the current table leader. This is the right choice for read-your-own-writes flows: for example, after a user updates their social-media profile, the following `GET /profile` should return the new profile immediately. Sending that read to a follower could briefly return the previous profile while replication catches up.

```powershell
curl.exe -X PUT http://localhost:9081/tables/profiles/kv/alice `
  -H "Content-Type: application/json" `
  -d '{"displayName":"Alice Smith"}'
curl.exe http://localhost:9081/tables/profiles/kv/alice `
  -H "X-Read-Consistency: strong"
```

Choose **eventual consistency** when a short propagation delay is acceptable and lower latency or higher read capacity is more important. Eventual reads are load-balanced across healthy replicas, so they are a good fit for browsing other users' profiles, timelines, public counters, recommendations, or other content where seeing a version that is briefly behind is acceptable.

For example, after Alice changes her own profile, reading thousands of other public profiles does not require every request to wait for the profile table leader:

```powershell
curl.exe http://localhost:9081/tables/profiles/kv/bob `
  -H "X-Read-Consistency: eventual"
```

### Bounded-staleness reads

Bounded staleness sits between eventual and strong consistency. It allows the
Router to use a healthy replica while guaranteeing that the replica is no more
than a requested number of committed sequence positions behind the current
leader. This is a deterministic freshness bound for read-heavy workloads.

```powershell
curl.exe http://localhost:9081/tables/profiles/kv/bob `
  -H "X-Read-Consistency: bounded-staleness" `
  -H "X-Max-Sequence-Lag: 25"
```

For example, if the leader is at sequence `10,000` and the maximum lag is `25`,
only replicas at sequence `9,975` or higher are eligible. The response includes
`X-Read-From` and `X-Read-Sequence` for observability. The Router rejects a
missing or invalid lag bound and fails rather than returning a replica outside
the requested bound. Use this for catalogs, public content, dashboards, or
other reads where a small and explicit amount of staleness is acceptable.

This implementation uses sequence lag rather than local wall-clock time, so it
is deterministic even when machines have clock skew. See the [bounded-staleness
design](./design/bounded-staleness.md) for the routing algorithm and the
trade-offs of a future time-based bound.

### Session consistency: monotonic reads, causal ordering, and consistent prefixes

The `session` consistency mode provides more continuity than ordinary eventual
reads without requiring every request to use the leader. `monotonic` and
`consistent-prefix` are accepted aliases for the same implementation. Because
each table has its own leader and replication sequence, these guarantees apply
per table; they are not a single global ordering across all tables.

**1. Monotonic reads**

Guarantee: once a client has seen a version of a table, a later read in that
session never returns an older version.

Without this guarantee, requests can hit different replicas and appear to go
backward in time:

```text
Request 1 -> Replica A -> likes = 10
Request 2 -> Replica B -> likes = 8   (bad: the session went backward)
```

With session consistency, the second read returns `likes = 10` or a newer
value. Think: **“my own view of this table should never go backward.”**

A client may first read Alice's profile at sequence `120`. The Router returns
`X-Read-Sequence: 120` (or newer). The client sends that value on its next
request, and the Router only chooses a replica that has applied sequence `120`
or later.

```powershell
curl.exe http://localhost:9081/tables/profiles/kv/alice `
  -H "X-Read-Consistency: session"
# Response: X-Read-Sequence: 120
curl.exe http://localhost:9081/tables/profiles/kv/alice `
  -H "X-Read-Consistency: session" `
  -H "X-Read-After-Sequence: 120"
```

**2. Consistent-prefix reads**

Guarantee: if writes have an order, a session never sees a later write without
also seeing the earlier writes that precede it. For example:

```text
W1: Alice creates a post
W2: Bob comments "Nice post!"

Allowed: Alice's post; Bob's comment
Allowed: Alice's post only
Forbidden: Bob's comment; Alice's post is missing
```

Think: **“I should not see effects before their causes.”** In this database,
the same per-table sequence floor provides this ordering: if sequence `120` is
visible, the Router will not send the next session read to a replica at `119`.
The implementation does not claim a separate cross-table prefix protocol.

**3. Causal ordering for a session**

Together, monotonic reads and consistent prefixes provide a useful form of
causal ordering within a table: when one operation has already observed the
result of an earlier operation, later reads in that session will not move
backward or expose the later effect while hiding the earlier one. This is a
session guarantee, not a globally ordered view for every client. The current
implementation carries the per-table sequence token through
`X-Read-After-Sequence`; it does not implement a full cross-table causal
dependency tracker.

A chat application is a good example. Suppose Alice sends two messages:

```text
M1: Alice sends "Are we still on for dinner?"
M2: Alice sends "I will book the table."
```

After the chat session has seen `M2`, it must not later display `M1` as
missing, and it should not show `M2` before `M1`. Session consistency gives the
conversation that natural order while allowing reads to use a caught-up
replica instead of routing every message-history read to the leader. A small
replication delay is acceptable because the user is not asking whether the
entire system has one immediate, globally agreed answer; they need their own
conversation to remain coherent.

This use case does **not** need linearizability. Linearizability would require
each read and write to appear to take effect atomically in real time for all
clients, which adds coordination and latency. Use strong/leader-routed reads
instead when the answer must reflect the latest committed state immediately,
such as confirming a payment, enforcing a unique allocation, or showing an
account balance after a transfer.

`session`, `monotonic`, and `consistent-prefix` all require a replica at or
beyond the client's sequence token. If no healthy replica has reached it, the
Router uses the leader when the leader has reached it; otherwise the request
fails instead of returning an older state.

Use the `session` name—or either alias—when the client must not see this table
go backward and must observe its committed writes in order. All aliases return
the selected replica's sequence in `X-Read-Sequence`; clients should persist it
and send it back as `X-Read-After-Sequence`.

The trade-off is:

| Choice | Route | Best for | Trade-off |
|---|---|---|---|
| `strong` | Current table leader | Read-your-own-writes, account settings, checkout state, immediate confirmation | Leader latency and capacity; fails if the leader cannot be discovered |
| `eventual` | Any healthy replica | Public profiles, feeds, recommendations, counters, high-volume browsing | A recently committed write may not be visible yet |
| `bounded-staleness` | Healthy replica within `X-Max-Sequence-Lag` of the leader | Catalogs, public content, dashboards, and reads with an explicit freshness bound | Requires a lag bound; may fail while replicas catch up |
| `session` (aliases: `monotonic`, `consistent-prefix`) | Healthy replica at or beyond `X-Read-After-Sequence` | Per-table read-your-own-writes, no backward movement, and ordered effects | Requires sequence tokens; may wait, use the leader, or fail while replicas catch up |

Reads carrying a `transactionId` always go to the relevant table leader, regardless of the header, so the transaction can see its own staged writes. Uncommitted writes are not replicated to followers or published to the change log before `COMMIT`.

The Router accepts four semantic `X-Read-Consistency` levels on HTTP and SQL requests (six accepted names including aliases):
`eventual` (the default), `session`, `monotonic` (an alias for `session`),
`consistent-prefix` (also an alias for `session`), `bounded-staleness`, and `strong`. The SQL console
currently exposes an `Eventual`/`Strong` selector; clients that need session
semantics can send the session header directly with their SQL request. Writes and
transaction control remain leader/coordinator routed. Direct database-node requests
bypass Router load balancing and read the local replica.

Router-served reads include `X-Read-From` in the response headers so clients can see which database node returned the data.

See [design/consistency-levels.md](./design/consistency-levels.md) for session and
strong/eventual routing, and [design/bounded-staleness.md](./design/bounded-staleness.md)
for bounded-staleness behavior.

Clients should connect to the Router port rather than selecting a table leader
manually. The Router forwards distributed transaction requests to a coordinator; the coordinator then contacts each affected table leader.
### Monitoring page

Open the Router monitoring page at `/monitoring` (for example, `http://localhost:9081/monitoring`). It refreshes every three seconds and displays:

- configured nodes and reachability;
- each discovered table;
- the role, term, and leader reported by every node for each table;
- ownership changes during elections and rebalancing;
- per-table disk size in KB on each node and total node storage;
- active and queued request counts.

The page uses `GET /monitoring/api/status` for its live JSON data. It is a Router-local operational view and does not change database state.

Request-admission metrics are scoped to POST /sql only. The SQL middleware inspects the statement: SELECT and SHOW use the read pool, while other SQL statements use the write pool. Each node has independent read and write pools, defaulting to 5,000 concurrent requests each. Monitoring and other HTTP endpoints bypass these pools, so operational polling does not consume SQL capacity.
### SQL console through the Router

The Router exposes the same ergonomic SQL console as the database node. Open it through the Router instead of a database port:

```text
http://localhost:9081/sql-console
http://localhost:9082/sql-console
http://localhost:9083/sql-console
```

The Router serves the console page and its JavaScript/CSS assets, then forwards the console's `POST /sql` requests. It reads the table names from the SQL statement, applies the selected consistency level, and returns the selected node in the `X-Read-From` response header. The console offers eventual and strong routing controls; session, monotonic, and consistent-prefix routing are available through the HTTP header contract. The browser therefore stays connected to the Router while table leadership and replica ownership remain internal cluster details.
### Calling the database through the Router

When using Docker Compose, send requests to the Router port instead of the database port:

```text
Router A: http://localhost:9081
Router B: http://localhost:9082
Router C: http://localhost:9083
```

For example, create a table and write/read a value through Router A:

```powershell
curl.exe -X POST http://localhost:9081/tables/users
curl.exe -X PUT http://localhost:9081/kv/users/alice `
  -H "Content-Type: application/json" `
  -d '{"name":"Alice","active":true}'
curl.exe http://localhost:9081/kv/users/alice
```

The request can enter through any Router. This request sent to Router C is forwarded to whichever node is currently the leader of `users`:

```powershell
curl.exe -X PUT http://localhost:9083/kv/users/bob `
  -H "Content-Type: application/json" `
  -d '{"name":"Bob"}'
```

SQL is also sent to the Router. The Router identifies the table from the SQL statement and forwards the request to that table's leader:

```powershell
curl.exe -X POST http://localhost:9081/sql `
  -H "Content-Type: application/json" `
  -d '{"sql":"SELECT * FROM users"}'
```

Applications should use the Router URL as their database endpoint and do not need to know the current table leader. If a leader election changes ownership, the Router refreshes its cached route automatically. Database ports (`8080`) remain node-to-node and direct database endpoints; client traffic should normally use the Router ports.

## Distributed transactions
### Leader-only transaction writes

Transactional writes remain owned by their table leaders. A transaction ID must
be registered on every node so the leader of each target table can stage the
write. At COMMIT, the coordinator gathers staged operations from the participating
leaders and runs two-phase commit.

Routing all statements to the node that received BEGIN is not sufficient, because
that node may not lead every table in the transaction. The Router therefore
combines transaction registration, table-leader routing, and coordinator-side
operation collection. A transaction ID used on a node that did not create or
register it returns transaction not found. The Router removes registrations from
all nodes after COMMIT or ROLLBACK.

Distributed transactions are supported when one transaction writes to multiple tables. A coordinator groups the staged writes by table, discovers each table leader, runs the prepare phase on every participant, and sends commit only after every participant has acknowledged prepare. Each participant applies its batch through the normal WAL, memtable, index, and change-log path.

![Distributed transaction two-phase commit](./docs/distributed-transaction-2pc.svg?raw=true)

The client API is:

- `POST /distributed-transactions`: begin a distributed transaction.
- `PUT /distributed-transactions/{id}/writes`: stage `{ table, key, value, isDeleted }`.
- `POST /distributed-transactions/{id}/commit`: run 2PC and return `committed`, `aborted`, or `in-doubt`.
- `DELETE /distributed-transactions/{id}`: abort the coordinator transaction.
- `GET /distributed-transactions/{id}`: inspect durable transaction state.
- `POST /distributed-transactions/{id}/recover`: request recovery/status evaluation.
- `GET /distributed-transactions/metrics`: inspect 2PC phase counters and prepared/active counts.

A successful path is: discover all leaders → prepare every participant → commit every participant. If discovery, prepare, or a prepare timeout fails, already-prepared participants receive abort and no staged value becomes visible. If a failure occurs after prepare has succeeded, 2PC can enter an `in-doubt` state: the coordinator must recover the decision before participants can safely finish. Prepared state and coordinator decisions are journaled, idempotent phase retries are supported, and abandoned coordinator transactions are expired automatically. The complete protocol and failure matrix are documented in [design/distributed-transactions.md](./design/distributed-transactions.md).
### Transparent SQL promotion

SQL transactions keep the normal BEGIN, statement, and COMMIT contract. A one-table transaction uses the local path. When staged writes target tables whose leaders are on different nodes, COMMIT automatically promotes the write set to the distributed coordinator and runs 2PC. Multiple tables led by the same node remain a local commit. Applications do not need a separate distributed transaction API or special SQL syntax.

Example: BEGIN, insert into user, insert into account, then COMMIT. The coordinator discovers both table leaders, prepares both participants, and commits both batches.

### Coordinator selection and failure behavior

The coordinator is the node that receives the transaction creation request; it
is not separately elected by Raft. For SQL, the Router remembers the node that
received `BEGIN` and routes `COMMIT`/`ROLLBACK` back to that node. For the
explicit distributed-transaction API, the node receiving
`POST /distributed-transactions` owns the coordinator state.

At commit, the coordinator groups staged writes by table, discovers each
current table leader through `/raft/tables/{table}/state`, groups tables led by
the same node, prepares every participant, journals `committing`, and then
sends commit to every prepared participant:

```text
Client -> Router -> coordinator
                     |
                     +-> discover table1 leader -> prepare -> commit
                     +-> discover table2 leader -> prepare -> commit
```

If the coordinator fails before prepare, the client sees a failed request and
no prepared value should be visible. If it fails during prepare, some
participants may remain prepared but invisible. If it fails after the
`committing` decision is journaled, some participants may commit while others
remain prepared; the result is `in-doubt` until the durable decision is
recovered.

There is currently no coordinator election or automatic coordinator failover.
The internal HTTP client has an infinite timeout, so a hung participant call
is not automatically detected by a configured phase timeout. The coordinator
journal and local recovery can replay durable committing work known to that
node, but they do not yet provide complete cluster-wide recovery after
permanent coordinator loss. The one-hour cleanup task expires abandoned active
transactions; it does not safely abort transactions that already passed
prepare.
### 2PC happy path

![2PC happy path](./docs/distributed-transaction-happy-path.svg?raw=true)

1. The Router sends the transaction request to a coordinator.
2. The coordinator groups writes by table and discovers each table leader.
3. It sends `prepare` to every participant. Participants journal the write set but do not expose it to reads.
4. After all prepare acknowledgements, the coordinator journals `committing`.
5. It sends `commit` to every participant. Each participant uses the normal WAL, memtable, index, and change-log path.
6. The coordinator returns `committed` only after all participants acknowledge.

### 2PC failure path and recovery

![2PC failure path](./docs/distributed-transaction-failure-path.svg?raw=true)

- Failure during leader discovery or prepare: already-prepared participants receive abort and the result is `aborted`.
- Failure after prepare: the coordinator returns `in-doubt`; prepared participants do not guess whether to commit or abort.
- Participant restart: its journal reloads prepared data and automatically replays journaled committing decisions.
- Repeated prepare or commit: requests are idempotent and return the existing decision.
- Abandoned active transactions: the background cleanup service expires them after one hour.
- Operational recovery: query `GET /distributed-transactions/{id}`, call `POST /distributed-transactions/{id}/recover`, and inspect `GET /distributed-transactions/metrics`.

The complete protocol, atomicity boundary, recovery model, and failure matrix are documented in [design/distributed-transactions.md](./design/distributed-transactions.md).

### 3PC: when it is useful and why this project uses 2PC

Three-phase commit (3PC) is a refinement of two-phase commit intended to
reduce the period in which participants can remain blocked when the
coordinator fails. Its phases are:

```text
1. CanCommit?  -> participants vote yes/no
2. PreCommit   -> coordinator tells participants that commit is inevitable
3. DoCommit    -> participants make the values visible
```

The extra `PreCommit` state gives participants more information about whether
it is safe to proceed after a coordinator failure. Under its timing and failure
assumptions, a participant can use timeouts and state to avoid waiting forever
for the coordinator's final decision. 3PC is appropriate when a system has a
small, known participant set, bounded network delays and failure detection, and
non-blocking commit is more important than the extra round trip and protocol
complexity.

3PC is not a general replacement for consensus. Network partitions, delayed
messages, coordinator/participant recovery, and inaccurate failure detection
can still make the assumptions unsafe. It also does not provide a global
ordering service or replace Raft. Systems that must tolerate arbitrary network
partitions usually combine consensus, replicated logs, or application-specific
recovery rather than relying on 3PC alone.

This project deliberately uses 2PC because it is sufficient for the current
cross-table transaction model. The coordinator already journals its decision,
participants durably journal prepared writes, phase requests are idempotent,
and recovery exposes an explicit `in-doubt` result instead of claiming success
when the outcome is unknown. The table leaders are already replicated by their
individual Raft groups, and the project does not require a non-blocking commit
under coordinator failure. Adding 3PC would add another durable participant
state and another network round without solving the separate problem that
independent table Raft groups do not form one global consensus group.

Use 2PC here when one transaction must atomically update tables led by
different nodes. 3PC would become worth evaluating only if measured
coordinator-blocking time became a dominant operational problem and the
cluster could enforce the stronger timing, failure-detection, and recovery
assumptions that 3PC needs.

## Data Model

The database stores named tables. Each table is an independent key/value LSM
tree with string keys and string values. The SQL layer treats the `value`
column as JSON: SQL writes must provide valid JSON, and SQL reads can filter by
the full JSON value or by dot-path properties inside that value.

Deletes are represented as tombstones so inserts, updates, and deletes can all
move through the same write path inside that table.

Keys are ordered using ordinal string comparison. Range queries use that same
ordering.

The default table is named `kv`, and the old `/kv` endpoints still target that
default table. Additional tables are created explicitly and live under their own
storage directories.

The SQL layer exposes every table with the same two logical columns: `key` and
`value`.

### SQL Engine

The SQL engine lives under `Sql/` and is intentionally layered above the
key/value database. It does not read or write WAL files, memtables, or SSTables
directly. Instead, it parses SQL into a small statement model and executes those
statements through the same `LsmStore` and `TransactionManager` methods used by
the REST API.

The main pieces are:

- `SqlParser`: tokenizes and parses supported SQL text into statement objects.
- `SqlEngine`: validates and executes parsed statements against `LsmStore` or
  `TransactionManager`.
- `SqlEndpoints`: exposes `POST /sql` and converts parser/execution errors into
  HTTP responses.

The SQL layer maps schemaless database tables to the following logical shape:

```sql
table_name(key string, value json)
```

This means SQL is a user-facing query language for the existing key/value store,
not a separate storage engine or a full relational schema system. SQL `INSERT`
and `UPDATE` values must be valid JSON documents.

### Relational tables

Relational tables add schema validation on top of the existing key/value tables.
Create one with a typed primary key and JSON-backed columns. Standard `CREATE TABLE (...)` syntax is supported; `CREATE RELATIONAL TABLE` remains an alias:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    active BOOLEAN
)
```

Rows continue to use the existing physical shape: the primary key is the table
key, while the other columns are properties in the JSON value:

```text
key:   "42"
value: {"name":"Ada","active":true}
```

The schema is stored in `data/catalog.json` under `relationalTables`. Relational
writes are validated before entering the WAL. The validator rejects invalid
primary-key types, missing `NOT NULL` columns, unknown properties, JSON values
with the primary key duplicated, and property type mismatches. Nullable columns
may be omitted or set to `null`. The primary key cannot be updated; change it
with an explicit delete followed by an insert.

Supported column types are `TEXT`, `INT`, `BIGINT`, `BOOLEAN`, and `DOUBLE`.
Existing `CREATE TABLE` tables remain schemaless and keep their current behavior.
See [the relational-table design](./design/relational-tables.md) for catalog
format, validation rules, and current limitations.

### Vector tables

Vector tables keep embeddings in ordinary JSON rows and maintain a derived
HNSW index on each replica. The index is rebuilt from LSM rows during startup.
Vectors must have the declared dimension and contain only finite numbers.

```sql
CREATE TABLE VECTOR documents (embedding VECTOR(3));
INSERT INTO documents VALUES ('doc-1', '{"embedding":[0.1,0.2,0.3],"text":"example"}');
SELECT key, value FROM documents ORDER BY embedding <-> '[0.1,0.2,0.25]' LIMIT 10;
```

Search uses cosine distance and returns `key`, `value`, and `distance`.
`efSearch` currently defaults to at least 64; increasing it explores more of the
graph and can improve recall at the cost of query time.

#### Neighbor-search algorithm choices

KNN describes the task of ranking vectors by distance. An exact flat scan checks
every vector, while approximate nearest-neighbor (ANN) indexes inspect a smaller
candidate set and trade some recall for lower latency.

| Algorithm | Search tradeoff | Write and storage tradeoff |
| --- | --- | --- |
| Exact KNN / flat scan | Exact results; distance work grows linearly with row count | No graph to maintain and little index memory, but every query scans the collection |
| HNSW | Approximate graph traversal; usually strong recall with low query latency | Supports incremental inserts; the graph uses substantial RAM, and deletes leave tombstones until rebuild |
| IVF-Flat | Searches selected centroid buckets; recall is tuned by probing more buckets | Requires centroid training and possible retraining; vectors remain uncompressed |
| IVF-PQ | Compressed approximate distances reduce memory and scan cost | Requires training codebooks and accepts additional distance error |
| LSH | Hash buckets produce approximate candidates | Incremental updates are straightforward, but recall depends on hash tables and probes |
| Annoy / random projection trees | Fast approximate traversal, suited to read-heavy collections | Primarily build-oriented; frequent updates commonly need rebuilds |
| DiskANN | Graph search designed to keep more of a large index on SSD | Reduces RAM pressure, with more complex SSD and graph maintenance |

This database uses HNSW because it can accept writes incrementally, which fits
the write-heavy workload better than build-oriented indexes such as Annoy. It
does not require a training phase like IVF, and avoids the extra quantization
error and codebook lifecycle of IVF-PQ. Its memory use is the main cost. The
LSM row remains authoritative; the per-replica HNSW graph is a derived,
in-memory index rebuilt from table rows at startup. This keeps crash recovery
simple, at the cost of startup rebuild time and memory proportional to the
indexed vectors. Search is approximate, and the current implementation uses
cosine distance only. It does not yet provide metadata filtering during graph
traversal or a persistent graph snapshot.

For a production-sized collection, compare recall@k, p50/p95 query latency,
insert throughput, memory per vector, and restart rebuild time against exact
scan and IVF-Flat using representative embeddings. See
[the vector-table design](./design/vector-tables.md) for lifecycle details and
additional limitations.

#### HNSW implementation internals

The durable representation is still the ordinary table row. The row key is the
vector ID, and the JSON `value` contains the embedding and any application data.
WAL, memtables, SSTables, change-log replication, and table snapshots store or
transfer those rows through the existing LSM paths. **The HNSW graph is not
stored in SSTables:** it is a derived in-memory index maintained separately on
each replica. The row remains authoritative if a process stops between a row
write and its index update.

Each in-memory node contains the row key, a `double[]` copy of its embedding, a
deleted flag, and neighbor lists for the levels the node belongs to. The graph
uses `M=16` and `efConstruction=200` by default. A new node receives a random
level: each additional level is selected with probability `1/M`, so most nodes
live only at level 0 and progressively fewer nodes appear on higher levels.
The highest-level node is the graph entry point.

On insert, HNSW starts at the entry point. It greedily follows a closer neighbor
on levels above the new node's level, then searches each shared level with a
construction candidate beam of `efConstruction`. It links the new node to up to
`M` closest candidates and adds reciprocal links. When a node accumulates more
than `2*M` neighbors at a level, the implementation keeps its `M` closest
neighbors. A node placed above the previous maximum level becomes the new entry
point. Updates mark the old node deleted and insert a replacement with the same
row key.

Search starts at the highest-level entry point and greedily moves to a closer
neighbor, descending one level at a time. At level 0 it explores a candidate
beam, ranks the candidates by cosine distance, and returns the best `k`. SQL
sets the beam to `max(64, LIMIT)`. The beam is not a promise that exactly that
many nodes are visited. The graph avoids scanning all rows in typical cases,
but results are approximate and query cost/recall depend on graph shape and
beam width. Distances for returned candidates are computed from their vectors;
the approximation is in which candidates the graph finds.

Cosine distance is calculated as `1 - dot(a,b) / (|a| * |b|)`. For nonzero
vectors it is a floating-point value from 0 (same direction) through 1
(orthogonal) to 2 (opposite direction). The implementation assigns distance 0
when both vectors are zero and 1 when only one is zero. It validates that
embeddings have the declared dimension and finite numeric components before
indexing them.

Deletes mark graph nodes as tombstones so traversal can still pass through
them, but search filters them from results. Tombstones remain until a restart
or index rebuild. At startup, the database scans the table's live LSM rows and
recreates the graph; no graph snapshot is currently persisted. This trades
simple recovery for startup work and RAM proportional to the collection. With
the current `double[]` storage, 100,000 vectors of 768 dimensions use about
586 MiB for the vector arrays alone, before node, edge, JSON row, and runtime
overhead.

##### Back-of-the-envelope memory sizing

The embedding-array estimate is `row count × dimensions × 8 bytes`, because
each component is stored as a 64-bit `double`. The table below converts that
payload to MiB/GiB and excludes array headers, HNSW nodes and links, dictionaries,
LSM buffers/cache, and the JSON rows themselves:

| Vectors | Dimensions | Embedding arrays only |
| ---: | ---: | ---: |
| 10,000 | 768 | about 58.6 MiB |
| 100,000 | 768 | about 586 MiB |
| 100,000 | 1,536 | about 1.14 GiB |
| 1,000,000 | 768 | about 5.72 GiB |

For another rough scale check, `M=16` gives up to roughly 16–32 neighbor
references per node at level 0 in this implementation. At 100,000 nodes, that
is approximately 12–24 MiB of 8-byte references before higher-level links and
the per-node `List<Node>` objects and backing arrays. This is only a graph-link
estimate: actual degree and managed-heap overhead vary with the data and runtime.
Plan RAM above the embedding-only figure, then measure the process with the
intended dimensions, row count, and write/search mix before choosing a capacity.

#### Vector SQL statements

Vector tables use the following SQL forms. Run each statement separately in the
SQL console:

```sql
CREATE TABLE VECTOR demo_vectors (embedding VECTOR(3));

INSERT INTO demo_vectors (key, value)
VALUES ('item-1', '{"embedding":[1,0,0],"label":"matched"}');

SELECT key, value
FROM demo_vectors
ORDER BY embedding <-> '[1,0,0]'
LIMIT 10;

UPDATE demo_vectors
SET value = '{"embedding":[0,0,1],"label":"updated"}'
WHERE key = 'item-1';

DELETE FROM demo_vectors WHERE key = 'item-1';
DROP TABLE demo_vectors;
```

- `CREATE TABLE VECTOR name (embedding VECTOR(dimension))` creates a vector
  table with one embedding column and a fixed dimension.
- `INSERT INTO name (key, value) VALUES (...)` stores a row. Omitting the column
  list uses the same `key, value` pair. `value` must be JSON with the declared
  embedding property.
- `SELECT ... FROM name ORDER BY embedding <-> '[...]' LIMIT k` performs ANN
  search. The embedding column, query dimension, and `LIMIT` (1 through 10000)
  are validated. Results currently contain `key`, `value`, and `distance`.
- Point/range `SELECT` by key, `UPDATE ... SET value ... WHERE key = ...`,
  `DELETE ... WHERE key = ...`, and `DROP TABLE` use the existing table SQL
  behavior. Updates and deletes maintain the derived graph.
- Search is cosine-only. SQL does not yet expose `efSearch`, metadata predicates
  within ANN traversal, multiple embedding columns, or exact KNN mode. ANN
  searches use committed index state; staged transaction writes become
  searchable after commit.

### Views

Views are read-only named `SELECT` queries stored in the table catalog. They
share the existing storage engine and do not create a second set of rows or
SSTables. Each read evaluates the view definition against the current base
table data:

```sql
CREATE VIEW gold_users AS
SELECT key, value FROM users WHERE value.tier = 'gold';

SELECT * FROM gold_users;
```

A view is exposed as a catalog object with `kind: "view"`; physical tables use
`kind: "kv"`, `kind: "document"`, or `kind: "relational"`. Views cannot be written to and have no independent WAL,
SSTables, indexes, or table leader. The definition is stored in
`data/catalog.json` under `views`. See [the views design](./design/views.md) for
catalog format, execution semantics, and current limitations.

Peer metadata discovery repairs missed table and view definitions after transient failures or node restarts. See [the metadata discovery design](./design/metadata-discovery.md).

![Cluster metadata discovery and recovery loop](./docs/metadata-discovery-loop.svg)
Example relational-table workflow:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    active BOOLEAN
);

INSERT INTO users (id, name, active) VALUES (42, 'Ada', TRUE);

SELECT id, name, active FROM users WHERE id = 42;

UPDATE users SET name = 'Grace' WHERE id = 42;

DELETE FROM users WHERE id = 42;
```

The query returns standard relational columns:

```json
{
  "statementType": "SELECT",
  "transactionId": null,
  "rowsAffected": 1,
  "rows": [
    {
      "id": "42",
      "name": "Ada",
      "active": "True"
    }
  ],
  "message": null
}
```

A terminal-style ANSI SQL example is shown below:

![ANSI SQL relational table query response](./docs/relational-table-ansi-crud.svg)

Submit SQL with:

```json
{
  "query": "SELECT key, value FROM users WHERE value.tier = 'gold'",
  "transactionId": null
}
```

Supported statements:

- `CREATE TABLE users`
- `SHOW TABLES` (returns `table`, `kind`, `leader`, and `leaderUrl` columns)
- `CREATE TABLE IF NOT EXISTS users`
- `CREATE VIEW gold_users AS SELECT key, value FROM users WHERE value.tier = 'gold'`
- `SELECT * FROM gold_users`
- `CREATE INDEX idx_users_tier ON users (value.tier)`
- `CREATE INDEX idx_users_tier_fast ON users (value.tier) USING FASTWRITE` for write-heavy exact-value indexing
- `BEGIN` or `BEGIN TRANSACTION` (defaults to `READ COMMITTED`)
- `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED`
- `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ`
- `BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE`
- `COMMIT` or `COMMIT TRANSACTION`
- `ROLLBACK` or `ROLLBACK TRANSACTION`
- `INSERT INTO users (key, value) VALUES ('user:1001', '{"name":"Ada","tier":"gold"}')`
- `INSERT INTO users VALUES ('user:1001', '{"name":"Ada","tier":"gold"}')`
- `SELECT * FROM users WHERE key = 'user:1001'`
- `SELECT key, value FROM users WHERE key BETWEEN 'user:1000' AND 'user:1999' LIMIT 100`
- `SELECT key FROM users WHERE key >= 'user:1000' AND key <= 'user:1999'`
- `SELECT key FROM users WHERE value = '{"name":"Ada","tier":"gold"}'`
- `SELECT key, value FROM users WHERE value.tier = 'gold' LIMIT 100`
- `SELECT key FROM users WHERE key >= 'user:1000' AND value.name = 'Ada'`
- `UPDATE users SET value = '{"name":"Ada","tier":"platinum"}' WHERE key = 'user:1001'`
- `DELETE FROM users WHERE key = 'user:1001'`

`BEGIN` (or an explicit isolation-level `BEGIN`) returns a transaction id. Include that id in later `/sql` requests to
stage SQL writes in the transaction or read with staged changes overlaid on
committed data. `COMMIT` and `ROLLBACK` require the transaction id in the
request body.

Example transaction flow:

```json
{ "query": "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", "transactionId": null }
```

Then use the returned transaction id:

```json
{
  "query": "INSERT INTO users (key, value) VALUES ('user:1001', '{\"name\":\"Ada\",\"tier\":\"gold\"}')",
  "transactionId": "00000000-0000-0000-0000-000000000000"
}
```

A more complex read can project selected columns, scan a key range, and limit
the result:

```json
{
  "query": "SELECT key, value FROM users WHERE key >= 'user:1000' AND key <= 'user:1999' AND value.tier = 'gold' LIMIT 50",
  "transactionId": "00000000-0000-0000-0000-000000000000"
}
```

That range query is translated to the same bounded key scan used by
`GET /kv/range`, then the SQL engine overlays any staged writes or deletes from
the transaction before applying JSON value predicates, projection, and limit.
JSON property predicates use dot paths such as `value.profile.tier = 'gold'`.
String properties are compared to the SQL string literal directly; numbers,
booleans, and null compare to their JSON text such as `'42'`, `'true'`, and
`'null'`.

Value filters are evaluated by scanning the selected key range, so queries
should include key bounds when possible unless a matching JSON value index
exists. Create a B+ tree index with `CREATE INDEX index_name ON table_name
(value.path)`; future SQL writes keep that index current, and startup rebuilds
it from committed table data.

This is not a full relational SQL database. Tables do not define custom columns
yet, and JOIN support is limited to inner equi-joins on matching table keys;
arbitrary predicates are not supported. SQL is currently another way to call the
existing table/key/value operations with optional B+ tree indexes for JSON value
equality filters.

### B+ Trees and JSON Value Indexes

![Disk-backed B+ tree JSON value index architecture](./docs/bplus-tree-index-architecture.svg?raw=true)

JSON value indexes live under `Indexes/` and are intentionally separate from the
LSM tree implementation. The storage engine remains a key/value LSM tree; the
index module is an auxiliary lookup structure used by SQL when a query filters
on the full JSON `value` document or a JSON property path.

Create an index with:

```sql
CREATE INDEX idx_users_tier ON users (value.tier)
```

Each index definition targets one table and one JSON path:

- `value`: indexes the full JSON document text.
- `value.tier`: indexes a top-level JSON property.
- `value.profile.city`: indexes a nested JSON property.

Each index is backed by a disk-persisted B+ tree. Internal pages contain
separator keys and child page ids. Leaf pages contain sorted `(indexedValue,
rowKey)` entries. Storing duplicates as separate leaf entries avoids one giant
in-memory posting list for low-cardinality properties such as `tier`. Leaf pages
are linked in sorted order, so the structure can later support ordered index
scans without changing the public index API.

The current SQL planner uses indexes for equality predicates:

```sql
SELECT key, value FROM users WHERE value.tier = 'gold'
```

If a matching index exists, the SQL engine asks the B+ tree for candidate row
keys, reads those rows from the table by key, and rechecks the JSON predicate
before applying projection and `LIMIT`. The recheck keeps the LSM table as the
authority even if an index has stale candidates during a future implementation
change. If no matching index exists, SQL falls back to the bounded key scan path.

Dump index structures with:

```text
GET /indexes/btrees
GET /indexes/{indexName}/btree
```

The dump returns the index metadata plus the B+ tree order, height, page count,
recursive root page, and linked leaf page entries. A trimmed response looks like:

```json
{
  "name": "idx_users_tier",
  "table": "users",
  "path": ["tier"],
  "tree": {
    "order": 32,
    "height": 1,
    "pageCount": 1,
    "root": {
      "pageId": 1,
      "kind": "leaf",
      "level": 0,
      "keys": ["gold", "silver"],
      "children": [],
      "entries": [
        { "key": "gold", "values": ["user:1001"] },
        { "key": "silver", "values": ["user:1002"] }
      ]
    },
    "leaves": [
      {
        "pageId": 1,
        "ordinal": 0,
        "entries": [
          { "key": "gold", "values": ["user:1001"] },
          { "key": "silver", "values": ["user:1002"] }
        ]
      }
    ]
  }
}
```

Index definitions and index pages are durable:

- Definitions are stored in `data/indexes/catalog.json`.
- Each index has `data/indexes/{indexName}/metadata.json`.
- Each index stores B+ tree pages under `data/indexes/{indexName}/pages/`.
- Startup opens existing B+ tree metadata and pages instead of rebuilding the
  whole index.
- If a catalog definition exists but page metadata is missing, the index is
  rebuilt from committed table data.
- The LSM table remains the source of truth for actual rows.
- There is not yet a separate index WAL; page writes are persisted after the
  committed table write succeeds.

Indexes are maintained after committed writes succeed:

- Direct database puts replace the old indexed value with the new one.
- Deletes remove the old row key from matching indexes.
- SQL `INSERT` and `UPDATE` go through the same database write path.
- Transaction commits update indexes after the committed batch is applied.
- Replicated changes update indexes after the follower applies the leader's
  change to the local table.

JSON values are normalized for comparison the same way SQL predicates compare
them. String properties compare to their string value. Numbers compare to their
JSON number text. Booleans compare to `true` or `false`, and JSON null compares
to `null`. Objects and arrays compare by their raw JSON text.

The index module is deliberately small and modular:

- `DiskBackedBPlusTree`: page-based B+ tree persisted under an index directory.
- `JsonValueAccessor`: extracts comparable values from JSON documents.
- `JsonValueIndexStore`: stores index definitions, opens or creates disk-backed
  trees, updates indexes on writes, and serves indexed equality lookups.

Current limitations:

- Indexes accelerate equality predicates only.
- Index pages are JSON files rather than fixed-size binary database pages.
- There is no index WAL yet, so a crash between table commit and index page
  update can require rebuilding the affected index from table data.
- There is no cost-based optimizer; SQL uses an index only when the predicate
  path exactly matches an index definition.
- Transactional reads do not use committed indexes because staged writes must be
  overlaid first.



For write-heavy workloads, create the same exact-value index with `USING FASTWRITE`. This stores index entries through the LSM/SSTable path, so updates become append/flush/compact operations instead of synchronous B+ tree page rewrites:

```sql
CREATE INDEX idx_users_tier_fast ON users (value.tier) USING FASTWRITE;
```

![FASTWRITE exact-value index write and lookup architecture](./docs/fastwrite-index-architecture.svg)

FASTWRITE keeps the source table authoritative and stores one internal row for each `(indexedValue, rowKey)` pair. For example, a `gold` value for row `user:1` is encoded as:

```text
Base64("gold") U+001F Base64("user:1")  ->  empty value
```

The write path extracts the old and new JSON values, appends a tombstone for the old index key and a put for the new key, then lets the normal WAL, memtable, flush, SSTable, Bloom-filter, sparse-index, and compaction pipeline persist the change. Equality lookup seeks the encoded value prefix, reads matching row keys, and rechecks the source rows before returning results. This favors write throughput and bounded memory; it does not provide ordered range scans like the B+ tree implementation.

### Choosing an index

Use the index type that matches the access pattern:

| Index | Best for | Write behavior | Read behavior |
|---|---|---|---|
| B+ tree | Mature exact-value lookups and future ordered scans | Synchronous page updates | Exact lookup and ordered structure |
| `FASTWRITE` | High-ingest exact-value lookups | WAL/memtable appends, flushes, and compaction | Encoded-value prefix seek, then source-row validation |
| Full-text | Terms, phrases, AND/OR, and ranked text search | Posting mutations through an internal LSM table | Term range reads plus scoring and phrase checks |

B+ tree and `FASTWRITE` indexes answer equality predicates such as `value.tier = 'gold'`. Full-text indexes answer analyzed text queries such as `distributed database`; they are not interchangeable because tokenization and ranking are part of the search contract.

### Full-text search

Full-text search is a separate search paradigm over ordinary document or key/value tables. A search index is an inverted index whose posting records are stored in an internal normal LSM table. Consequently, the index uses the same WAL, memtable, bounded SSTables, Bloom filters, sparse indexes, and compaction path as application tables. The source table remains authoritative.

Create an index over JSON fields with SQL:

```sql
CREATE SEARCH INDEX articles_text
ON articles (value.title, value.body);
```

Query it from SQL:

```sql
SEARCH articles_text MATCH 'distributed database' LIMIT 20;
SEARCH articles_text MATCH '"database engine"' OPERATOR AND LIMIT 20;
```

The REST API provides the same capability:

```http
PUT /search/indexes/articles_text
Content-Type: application/json

{"table":"articles","fields":["value.title","value.body"]}
```

```http
POST /search/articles_text
Content-Type: application/json

{"query":"distributed database","operator":"or","limit":20,"offset":0}
```

Search uses Unicode letter/number tokenization so international text—such as accented Latin, Cyrillic, Arabic, Greek, or CJK content—is searchable instead of being discarded by an ASCII-only analyzer. Lowercasing makes searches case-insensitive while returned values remain unchanged. Search results are ranked because matching documents otherwise have no useful relevance order. BM25-style ranking rewards multiple and repeated matches, gives rarer terms more weight, and normalizes for field length; this generally places the most relevant documents on the first page. The current implementation uses k1 = 1.2, b = 0.75, and a fixed average-length approximation, so scores are intended for ordering rather than Elasticsearch-compatible numeric comparisons. Index updates are synchronous with source writes. Use `POST /search/{name}/rebuild` to reconstruct an index from the source table's current SSTable-backed rows. Fuzzy matching, stemming, wildcards, autocomplete, and aggregations are not part of the first version. See [the full-text search design](./design/full-text-search.md) for the posting layout and recovery model.
![Full-text search creation, SSTable storage, Raft propagation, and query flow](./docs/full-text-search-architecture.svg)

#### Internal inverted-index rows

For example, this index:

```sql
CREATE SEARCH INDEX articles_text
ON articles (value.title, value.body);
```

is stored as an internal LSM table named `__search_articles_text` under:

```text
data/search-indexes/articles_text/
├── wal.log
└── sstables/
    ├── sstable-....json
    ├── sstable-....bloom.json
    └── sstable-....index.json
```

An example source row:

```json
{"key":"article:1","value":{"title":"Distributed Database","body":"A database engine"}}
```

produces ordinary internal key/value postings like these:

```text
title␟distributed␟YXJ0aWNsZTox
→ {"frequency":1,"positions":[0],"length":2,"field":"title"}

title␟database␟YXJ0aWNsZTox
→ {"frequency":1,"positions":[1],"length":2,"field":"title"}

body␟database␟YXJ0aWNsZTox
→ {"frequency":1,"positions":[1],"length":3,"field":"body"}

body␟engine␟YXJ0aWNsZTox
→ {"frequency":1,"positions":[2],"length":3,"field":"body"}
```

The `␟` character represents the internal U+001F separator. The document key is Base64-encoded so arbitrary user keys cannot break the posting-key format. `frequency` supports scoring, `positions` supports phrase queries, and `length` supports field-length normalization in BM25-style ranking. These rows use the normal WAL, memtable, SSTable, Bloom-filter, sparse-index, and compaction paths, but remain hidden from user table listings. Search-index definitions are propagated through table Raft to peers; posting rows themselves are derived locally from each node's replicated source table rather than independently replicated.
### SQL JOINs

The SQL engine supports inner equi-joins between two tables when their keys match:

```sql
SELECT users.value, orders.value
FROM users
JOIN orders ON users.key = orders.key
LIMIT 100
```

A join returns only matching keys. Columns must be qualified with their table
name. `SELECT *` returns `users.key`, `users.value`, `orders.key`, and
`orders.value`. The current implementation scans up to 1,000 rows from each
input, limits output to `LIMIT` (1-1,000), and does not support joins inside
transactions.

For example:

```sql
INSERT INTO users VALUES ('u1', '{"name":"Ada"}')
INSERT INTO orders VALUES ('u1', '{"item":"Book"}')
SELECT * FROM users JOIN orders ON users.key = orders.key
```
JSON properties can also be used as join keys. The properties must exist and
have comparable JSON values on both sides; rows with missing properties are
skipped. Existing JSON-property indexes can be used to optimize future join
planning, while the current join implementation uses bounded scans:

```sql
SELECT users.key, orders.key
FROM users
JOIN orders ON users.value.customerId = orders.value.customerId
LIMIT 100
```

## HTTP API

- `GET /tables`: list tables.
- `PUT /tables/{table}`: create a table.
- `DELETE /tables/{table}`: drop a physical table and its catalog/storage metadata.
- `PUT /tables/{table}/kv/{key}`: create or update a value in a table.
- `GET /tables/{table}/kv/{key}`: fetch a single value from a table.
- `DELETE /tables/{table}/kv/{key}`: delete a key from a table.
- `GET /tables/{table}/kv/range?start=a&end=z&limit=100`: return table keys
  in sorted order.
- `GET /tables/{table}/stats`: inspect one table, including its on-disk `diskSizeBytes`.
- `GET /tables/{table}/snapshot`: bootstrap a follower from a table snapshot and applied sequence.
- `POST /raft/rebalance`: probe configured peers and persist a new healthy ownership assignment.
- `GET /metrics`: inspect aggregate and read/write-specific active and queued HTTP requests, the separate read/write limits, and total node `totalDiskSizeBytes`. Reads and writes default to independent 5,000-request pools and can be changed with `Lsm:MaxConcurrentReads` and `Lsm:MaxConcurrentWrites`.
- `POST /raft/membership/register`: register a peer and trigger ownership rebalancing.
- `PUT /kv/{key}`: create or update a value in the default `kv` table.
- `GET /kv/{key}`: fetch a single value from the default `kv` table.
- `DELETE /kv/{key}`: delete a key from the default `kv` table.
- `GET /kv/range?start=a&end=z&limit=100`: return default table keys in
  sorted order.
- `POST /transactions`: start a transaction.
- `PUT /transactions/{transactionId}/kv/{key}`: stage a transactional upsert.
- `DELETE /transactions/{transactionId}/kv/{key}`: stage a transactional delete.
- `GET /transactions/{transactionId}/kv/{key}`: read with the transaction's
  staged changes overlaid on committed data.
- `GET /transactions/{transactionId}/kv/range?start=a&end=z&limit=100`: range
  read with staged transaction changes overlaid on committed data.
- `PUT /transactions/{transactionId}/tables/{table}/kv/{key}`: stage a
  transactional upsert in a table.
- `DELETE /transactions/{transactionId}/tables/{table}/kv/{key}`: stage a
  transactional delete in a table.
- `GET /transactions/{transactionId}/tables/{table}/kv/{key}`: read a table
  with the transaction's staged changes overlaid on committed data.
- `GET /transactions/{transactionId}/tables/{table}/kv/range?start=a&end=z&limit=100`:
  range read a table with staged transaction changes overlaid on committed data.
- `POST /transactions/{transactionId}/commit`: commit staged writes.
- `DELETE /transactions/{transactionId}`: rollback and discard staged writes.
- `GET /indexes`: list current SQL JSON value indexes.
- `GET /search/indexes`: list full-text search indexes.
- `PUT /search/indexes/{name}`: create a full-text index over source-table JSON fields.
- `POST /search/{name}`: execute a ranked full-text query.
- `POST /search/{name}/rebuild`: rebuild a full-text index from its source table.
- `GET /indexes/btrees`: dump all current JSON value index B+ trees.
- `GET /indexes/{name}/btree`: dump one JSON value index B+ tree.
- `POST /sql`: execute a SQL statement against the table/key/value/index model.
  Supports `DROP TABLE name` and `DROP TABLE IF EXISTS name`.
- `GET /sql-console`: open the embedded browser SQL console.
- `GET /changes?fromSequence=0&limit=100`: replay committed change-log events.
- `GET /changes/stream?fromSequence=0`: stream committed changes as
  Server-Sent Events.
- `GET /changes-console`: open the embedded change-log stream console.
- `GET /raft/state`: inspect this node's Raft role, term, leader, and applied
  change sequence.
- `POST /raft/request-vote`: internal Raft vote RPC used by peers.
- `POST /raft/append-entries`: internal Raft heartbeat RPC used by the leader.
- `GET /stats`: inspect database and per-table statistics.

## Python Client

The repository includes a dependency-free Python client in `python_client/` for the HTTP and Server-Sent Events APIs. It supports tables, key/value reads and writes, transactions, SQL execution, prepared ANSI SQL statements for relational tables, statistics, change-log replay, and reconnectable change streaming.

Install it locally:

```text
cd python_client
python -m pip install .
```

Example:

```python
import logging
from lsmwrite_client import LsmWriteDbClient

logging.basicConfig(level=logging.INFO)
client = LsmWriteDbClient("http://localhost:8080")
client.put("user:1", "Ada")
print(client.get("user:1"))

transaction = client.begin()
transaction.put("user:2", "Grace")
transaction.commit()

for event in client.stream_changes():
    print(event)
```


Prepared ANSI SQL

For relational tables, `prepare()` provides reusable client-side parameterized ANSI SQL. Parameters are escaped and converted to SQL literals before the request is sent; existing `execute_sql(query)` calls remain supported.

```python
from lsmwrite_client import LsmWriteDbClient

client = LsmWriteDbClient("http://localhost:8080")
client.execute_sql("""
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    active BOOLEAN
)
""")

insert_user = client.prepare(
    "INSERT INTO users (id, name, active) VALUES (?, ?, ?)"
)
insert_user.execute(42, "Ada", True)

select_user = client.prepare("SELECT id, name, active FROM users WHERE id = ?")
print(select_user.execute(42))

client.prepare("UPDATE users SET name = ? WHERE id = ?").execute("Grace", 42)
client.prepare("DELETE FROM users WHERE id = ?").execute(42)
```

`execute_sql()` also accepts `parameters=[...]` and an optional transaction id:

```python
client.execute_sql(
    "SELECT * FROM users WHERE id = ?",
    parameters=[42],
)
```
A Docker-backed integration test is available at `python_client/run_integration_test.py`; it starts the local Compose cluster, discovers a leader, runs CRUD, transaction, SQL, change-log, and stats commands through the client, and tears the cluster down afterward.

The client uses the standard `logging` module under the `lsmwrite` logger. Request
method, URL, retry attempt, response status, elapsed time, transport failures,
stream reconnects, and SSE heartbeats are logged at appropriate levels. Transient
HTTP failures and transport errors are retried with exponential backoff. Change
stream reconnection resumes from the last received sequence.

## TCP SQL

The service also exposes a long-lived TCP SQL session endpoint, similar to a
small database wire protocol. It is line-oriented and keeps the transaction id
inside the connection, so after `BEGIN;` you can keep sending SQL over the same
socket until `COMMIT;`, `ROLLBACK;`, or disconnect.

Default listener:

```text
127.0.0.1:6543
```

Example session:

```text
CREATE TABLE users;
INSERT INTO users VALUES ('user:1001', '{"name":"Ada","tier":"gold"}');
CREATE INDEX idx_users_tier ON users (value.tier);
SELECT key, value FROM users WHERE value.tier = 'gold';
QUIT;
```

Protocol basics:

- The server sends `lsm> ` when it is ready for a statement.
- End each SQL statement with `;`.
- Multi-line statements are accepted; the prompt changes to `...> ` until `;`.
- Successful statements return `OK {json}`.
- Errors return `ERR "message"`.
- `QUIT;`, `EXIT;`, or `\q` closes the session.

### Run the Docker TCP SQL CLI

The easiest way to try the TCP SQL session protocol is the local PowerShell CLI
script. It requires Docker Desktop to be running.

From the repository root on Windows, run the `.cmd` launcher:

```powershell
.\scripts\tcp-sql-cli.cmd -Rebuild
```

The launcher starts the PowerShell script with a process-local execution policy
bypass, so you do not need to change your machine policy. If your PowerShell
already allows local scripts, you can run the script directly:

```powershell
.\scripts\tcp-sql-cli.ps1 -Rebuild
```

The script builds the Docker image when needed, starts the database container in
detached mode, waits for the TCP SQL listener, and then opens a colored CLI.
Enter each SQL statement with a trailing `;`. Type `exit`, `quit`, or `\q` to
disconnect.

If you start a statement without `;`, the CLI switches to the continuation
prompt `...>`. Finish that statement with `;` before starting the next SQL
command. Use `\clear` to discard an unfinished statement.

`SELECT` results are rendered as a table in the CLI. Other successful
statements are shown as compact `OK` summaries.

![TCP SQL CLI SELECT table output](./docs/tcp-sql-cli-select-table.png?raw=true)

If you see an error about `dockerDesktopLinuxEngine` or Docker being unreachable,
start Docker Desktop and wait until the Linux engine is running, then run the
same command again.

If the TCP connection closes unexpectedly, the CLI prints recent container logs
and keeps the container instead of deleting it, so you can inspect the database
process with Docker Desktop or `docker logs lsm-write-db-tcp-sql`.

Example:

```text
sql> CREATE TABLE users;
sql> INSERT INTO users VALUES ('user:1001', '{"name":"Ada","tier":"gold"}');
sql> CREATE INDEX idx_users_tier ON users (value.tier);
sql> SELECT key, value FROM users WHERE value.tier = 'gold';
+-----------+------------------------------+
| key       | value                        |
+-----------+------------------------------+
| user:1001 | {"name":"Ada","tier":"gold"} |
+-----------+------------------------------+
OK SELECT (1 row)
sql> exit
```

Use `-KeepContainer` if you want the detached database container to keep running
after the CLI exits:

```powershell
.\scripts\tcp-sql-cli.cmd -KeepContainer
```

Configure it with:

```json
{
  "TcpSql": {
    "Enabled": true,
    "Host": "127.0.0.1",
    "Port": 6543,
    "MaxQueryBytes": 65536
  }
}
```

## Components

### Write-Ahead Log

Every write is appended to a write-ahead log before it is applied to memory.
On startup, the log is replayed so acknowledged writes are not lost.

The WAL is cleared only after the current memtable has been successfully written to an SSTable. Compaction runs afterward and merges SSTables; WAL deletion does not wait for compaction because the flushed SSTable already contains the records needed for local recovery. If the process crashes before the flush completes, the WAL remains available for replay.

WAL integrity: every new WAL line is wrapped with its JSON payload and an uppercase SHA-256 checksum. On replay, the checksum is verified before the payload is applied; corrupted or partially written entries are ignored. Legacy unwrapped JSON WAL lines remain readable.

Each table has its own WAL under `data/tables/{table}/wal.log`. Direct `PUT`
and `DELETE` requests are written to that table's log as individual records.
Committed transaction writes are grouped by table and written as committed batch
records in the affected table WALs. On startup, each table replays individual
records and complete committed batches. A partial trailing batch line can be
left by a crash and is ignored instead of being replayed.

### Change Log Stream

Committed writes from all tables are also appended as JSONL events to bounded
`data/changelog-000000000000000000NN.log` segments. The change log is separate
from the WAL: the WAL protects local crash recovery, while the change log provides
a durable, ordered source that followers and other consumers can replay. Existing
`data/changelog.log` files remain readable for backward compatibility. When the
active segment reaches its size limit, the next event starts the next numbered
segment; an event is never split between files. Each event contains the global committed sequence number,
table, operation, key, value, tombstone flag, and commit timestamp. Direct
`PUT`/`DELETE`, SQL writes, and transaction commits all publish through the same
log because they all eventually call the storage engine write path.

The durable change-log files are organized as a legacy-compatible file followed
by bounded numbered segments:

```text
data/
+-- changelog.log                         # legacy file, read-compatible
+-- changelog-00000000000000000001.log   # immutable older segment
+-- changelog-00000000000000000002.log   # immutable older segment
+-- changelog-00000000000000000003.log   # active append segment
```

Consumers can replay recent changes with:

```text
GET /changes?fromSequence=120&limit=100
```

They can also subscribe to the live stream with:

```text
GET /changes/stream?fromSequence=120
```

The stream uses Server-Sent Events. The server first replays durable changelog
entries with `Sequence > fromSequence`, then keeps the HTTP response open and
sends new committed events as they happen. This lets another service store the
last sequence it processed and reconnect later without reprocessing older
changes.

Example event:

```text
id: 121
event: change
data: {"sequence":121,"operation":"put","key":"user:1001","value":"Ada","isDeleted":false,"committedAt":"2026-05-19T12:00:00Z","table":"users"}
```

![Segmented change-log tree](./docs/change-log-segment-tree.svg?raw=true)

The browser UI at /changes-console can replay or subscribe to the same stream
and display events in a table.

The server sends an SSE comment heartbeat (: heartbeat) every 15 seconds while
the connection is idle. Heartbeats keep proxies and load balancers from treating
the connection as inactive; they are comments, not change events, so followers
ignore them. If the connection closes, a follower reconnects with its last
successfully applied sequence. The leader then replays only events after that
sequence before resuming the live stream. A follower records the sequence only
after applying the event locally.

### Raft Leader Election

The Raft layer lives under `Raft/` and is responsible for leader election,
heartbeats, write gating, and follower replication. It is intentionally separate
from the storage engine.

Leader election:


Change-log subscription:


Each node has a role:

- Leader: accepts writes, appends them to the WAL and change log, and sends
  heartbeats to peers.
- Follower: rejects write requests with a `409 Conflict`, keeps serving reads,
  and subscribes to the leader's `/changes/stream` endpoint to apply committed
  changes locally.
- Candidate: starts an election when it has not seen a heartbeat within the
  election timeout.

The election protocol tracks terms and votes in `data/raft-state.json`.
Followers vote at most once per term. The elected leader sends heartbeat
messages through `/raft/append-entries`; if a node sees a newer term, it steps
down to follower.

Follower replication uses a persistent per-table worker. A closed or failed SSE connection is treated as a reconnect signal; the worker retries continuously and resumes from the last applied sequence. It uses the durable change-log stream rather than exposing
SSTables directly. Each follower stores the last leader change sequence it
applied in `data/raft-replication.json`, then reconnects with:

```text
GET {leaderUrl}/changes/stream?fromSequence={lastAppliedChangeSequence}
```

The follower first receives missed durable events and then continues with live
events delivered through the open SSE connection. This lets disconnected followers catch up from disk while connected followers receive new commits without polling. Applied changes preserve the leader's change sequence locally so the
replica can resume from the same sequence after restart. Change-log events carry
the table name, so followers create missing tables and apply each event to the
matching table-local LSM tree.
The stream is global to the leader's change-log namespace even though the
replication checkpoint is maintained per table. For example, if one leader
hosts two tables:

```text
Leader A change log:

sequence 5 -> table1
sequence 6 -> table2
sequence 7 -> table1
sequence 8 -> table2
```

A follower worker for `table1` reconnecting from sequence `5` receives the
shared stream, applies sequences `5` and `7`, and ignores the `table2` events:

```text
Follower table1 worker:

sequence 5 -> table1 -> apply
sequence 6 -> table2 -> ignore
sequence 7 -> table1 -> apply
sequence 8 -> table2 -> ignore
```

A worker for `table2` has its own table checkpoint and performs the symmetric
filtering. The request does not send a checkpoint to every shard, and a
sequence token is not ambiguous inside the worker: the worker already knows
which table it is replicating. The token means "resume this table's worker at
this position in the shared leader stream."

If the follower needs both tables from the same leader, the current
implementation creates two per-table workers and therefore may open two
streams that read the same changelog events:

```text
                 shared leader stream
                         |
              +----------+----------+
              |                     |
       table1 worker            table2 worker
       apply table1             apply table2
       ignore table2             ignore table1
```

This can duplicate network and parsing work, but it does not duplicate rows:
each worker filters by table, and each table-local store rejects an already
applied sequence. The current design therefore favors table-level leadership
and simple recovery over one multiplexed shard-level stream. A future
optimization could open one stream per follower/leader pair and dispatch each
change event to the appropriate table worker:

```text
Follower opens one stream from sequence 5
                         |
                 event dispatcher
                    /          \
             table1 state   table2 state
```

That optimization would remove redundant stream reads while retaining
per-table applied state and table-local leadership. It would not, by itself,
create a globally unique sequence authority across independent leaders.

Writes are gated at the API layer. Direct `PUT`/`DELETE`, transactional writes,
transaction commits, SQL writes, and SQL transaction control require the node to
be the current leader. Point and range reads remain available on followers.

Example three-node configuration for one node:

```json
{
  "Raft": {
    "Enabled": true,
    "NodeId": "node-a",
    "PublicUrl": "http://localhost:8081",
    "Peers": [
      { "NodeId": "node-b", "Url": "http://localhost:8082" },
      { "NodeId": "node-c", "Url": "http://localhost:8083" }
    ]
  }
}
```

### Transactions

Transactions live in the `Transactions/` code path and stage writes in memory.
Starting a transaction returns a transaction id. `PUT` and `DELETE` requests made
through that id update only the transaction's private table/key write set; they
are not written to any table WAL, are not applied to any memtable, and are not
visible to normal reads.

Reads inside a transaction see their own staged writes overlaid on top of the
committed table. A staged delete hides the committed value for that table/key
inside the transaction.

Write conflict detection is not implemented. If two transactions write the same
table/key, commits are serialized through the target table store mutex and the
later commit wins because it receives the newer global sequence number. Reads
for table/keys that were not staged in the transaction can also see changes
committed by other transactions after this transaction started.

Commit closes the transaction and sends its staged operations to the storage
engine. Operations are grouped by table, and each affected table appends its
committed batch to its own WAL before applying the records to its memtable, so a
restart can replay complete committed batches. Rollback simply discards the
in-memory write set.

If the server crashes before commit finishes writing a complete committed batch,
the transaction's staged writes are not restored on startup. This keeps
uncommitted changes from becoming durable.

#### ACID Properties

- Atomicity: transaction writes are staged in memory and become visible only
  when commit sends the full staged write set to the store as one batch. Rollback
  drops the staged write set.
- Consistency: committed records still go through the same key/value validation,
  table validation, sequence numbering, tombstone handling, WAL, memtable,
  flush, and compaction rules as direct writes.
## Transaction isolation levels

Transactions default to read committed and can select a level at `BEGIN`:

```sql
BEGIN;
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

- **Read committed**: each statement reads committed data as of that statement, with read-your-own-writes. It prevents dirty reads, but later reads may observe another transaction's commit.
- **Repeatable read**: the first point/range result is cached for the transaction, so repeating the same read is stable while staged writes remain visible. It is not MVCC snapshot isolation; different ranges can still observe phantoms.
- **Serializable**: serializable transactions use a manager-wide strict two-phase-locking gate held from `BEGIN` to `COMMIT`/`ROLLBACK`. This prevents interleaving among serializable peers, but blocks them and does not cover weaker transactions, other processes, replicas, or distributed participants.

2PL has a growing phase, where locks are acquired, and a shrinking phase after the transaction closes, where locks are released. LsmWriteDb uses one coarse lock rather than key/range locks to keep this implementation deadlock-free.

Snapshot isolation and MVCC are **not supported**. Repeatable read is a read cache, not a historical database snapshot. Linearizability is separate from transaction isolation: it requires each operation to appear atomic in real time. Use strong/leader-routed reads when the latest leader state is required.

See [design/transaction-isolation.md](./design/transaction-isolation.md) for guarantees, failure modes, and implementation details.

### Concurrent request examples: on-call doctor scheduling

Assume `doctor:ada` is the only on-call doctor and `slot:10:00` is an open appointment. Request A is the scheduling service and Request B is an on-call dashboard updating availability.

**Read committed**

```text
A: BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED
A: SELECT * FROM doctors WHERE key = 'doctor:ada' -> on_call=true
B: UPDATE doctors SET value = '{"on_call":false}' WHERE key = 'doctor:ada'
B: COMMIT
A: SELECT * FROM doctors WHERE key = 'doctor:ada' -> on_call=false
A: COMMIT
```

The second read sees B's committed change because each statement reads fresh committed state. This is useful when every decision is short-lived, but A can make a decision from the first result and act on a different state later. It does not prevent non-repeatable reads, phantoms, or a doctor being double-booked by a separate concurrent workflow.

**Repeatable read**

```text
A: BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ
A: SELECT * FROM doctors WHERE key = 'doctor:ada' -> on_call=true
B: UPDATE doctors SET value = '{"on_call":false}' WHERE key = 'doctor:ada'
B: COMMIT
A: SELECT * FROM doctors WHERE key = 'doctor:ada' -> on_call=true
A: COMMIT
```

A reuses its cached point read, so its two reads agree. The limit is that this is not a full database snapshot: A can still see B's commit through an unread key or a different range query. The cache also does not reserve the doctor; another transaction can update or book the same record, and conflicting writes remain last-commit-wins.

**Serializable**

```text
A: BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE
B: BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE -> waits
A: SELECT * FROM doctors WHERE key = 'doctor:ada' -> on_call=true
A: INSERT INTO appointments (key, value) VALUES ('slot:10:00', '{"doctor":"ada"}')
A: COMMIT
B: BEGIN completes
B: SELECT * FROM doctors WHERE key = 'doctor:ada' -> current committed state
```

Serializable peers do not overlap, so B cannot make a scheduling decision while A is running. The limit is scope: a `READ COMMITTED` or `REPEATABLE READ` transaction can still run concurrently, and the gate is local to one process/transaction manager. It does not coordinate other nodes, replicas, or distributed participants. Long serializable transactions also block other serializable transactions.
### Transaction buffer


Each transaction has a small private buffer. It does not copy the entire memtable. Internally, the buffer is a .NET `Dictionary<TransactionWriteKey, TransactionWrite>` keyed by table and key. A second write to the same key replaces the first staged write.

For example, if the memtable contains:

```text
A = 1
B = 2
C = 3
```

and a transaction stages:

```text
A = 10
D = 4
```

the transaction buffer contains only `A = 10` and `D = 4`. It does not contain copies of `B` or `C`.

### Comparison with stronger isolation levels

- **Snapshot isolation:** a transaction reads from one consistent snapshot of committed data. Later commits by other transactions are not visible to it, so repeated reads see the same versions. The tradeoff is extra version storage and cleanup work, plus possible write conflicts or long-lived snapshots that retain old data and consume memory or disk space.
- **Serializable isolation:** concurrent transactions produce the same result as if they had executed one at a time, usually through locking, validation, or serialization. The tradeoff is lower concurrency: transactions may block, deadlock, or be aborted and retried, and conflict tracking adds CPU and memory overhead.

Snapshot isolation is not implemented, and conflicting writes still use last-commit-wins semantics.
- Durability: commit appends each table's committed batch to that table's WAL
  before applying it to the memtable. After a restart, complete committed
  batches are replayed. Partial trailing batch records are ignored, so
  incomplete commits do not become durable.

### SQL Console

The service also serves a browser-based SQL console at `/sql-console`. It is
implemented in `SqlConsole/` and ships with the API, so there is no separate
frontend build step. The page posts queries to `/sql`, renders returned rows as a
table, stores recent queries in browser local storage, and keeps the active
transaction id in the console input. The sidebar exposes a transaction-isolation selector; the Begin button emits
`BEGIN TRANSACTION ISOLATION LEVEL ...` using the selected level. The selector is
saved in browser local storage and defaults to read committed.

![SQL console screenshot](./docs/sql-console.svg?raw=true)

The console has direct controls for `BEGIN`, `COMMIT`, and `ROLLBACK`. When
`BEGIN` returns a transaction id, the console stores it and sends it with later
queries until the transaction is committed, rolled back, or cleared. The sidebar
also includes clickable suggested queries for point reads, range reads, writes,
and transaction control.

### Change Log Console

The service also serves `/changes-console`, a browser page for replaying and
watching committed change-log events. Enter the last processed sequence, connect
to the stream, and the page shows replayed and live events in a table. It uses
`/changes/stream` for live updates and `/changes` for one-time replay.

![Change log console screenshot](./docs/change-log-console.svg?raw=true)

### Ordered MemTable

Each database table has its own memtable. The memtable must support sorted
iteration, so it is not a hash dictionary. It is an ordered in-memory table
keyed by string.

For this simple C# version, the implementation can wrap a `SortedDictionary`
because .NET's `SortedDictionary` is a tree-backed ordered map, not a hash table.
The rest of the code should depend on a small `MemTable` abstraction so the
internal structure can later be replaced with a skip list or B-tree without
changing the API or storage engine.

The memtable supports:

- single-key lookup,
- insert/update/delete by key,
- sorted range scans from `start` to `end`,
- snapshotting sorted records during flush.

### SSTables

Each table flush creates one immutable sorted Tier-0 logical run. The run is encoded as sorted
JSON blocks and partitioned into one or more bounded physical SSTable files under
the table's `sstables/` directory. Each physical data file has two sidecars:

- `.bloom.json`: a Bloom filter for definite-miss checks.
- `.index.json`: a sparse index with one entry per block, including key range,
  byte offset, byte length, record count, and checksum.

![SSTable block storage, Bloom filter, and sparse index layout](./docs/sstable-block-index-architecture.svg)

A physical SSTable file belongs to a logical run identified by its `-L##-R<run-id>-` filename prefix. Multiple physical files can share one run ID; a block is not itself an SSTable:

```text
SSTable run
+-- sstable-001
|   +-- block 1
|   +-- block 2
|   +-- sparse index + Bloom filter
+-- sstable-002
    +-- block 3
    +-- block 4
    +-- sparse index + Bloom filter
```

The block size is configurable with `Lsm:BlockSizeBytes` or the
`Lsm__BlockSizeBytes` environment variable. The maximum physical file size is
configurable with `Lsm:MaxSstableFileSizeBytes` or
`Lsm__MaxSstableFileSizeBytes`. The change-log segment size is configurable with
`Lsm:ChangeLogSegmentMaxBytes` or `Lsm__ChangeLogSegmentMaxBytes`. Defaults are
`16384` bytes per block and `67108864` bytes per SSTable or change-log segment.
These are targets: a single record
or block is never split, so an oversized record may exceed the file target.

### Sparse Index

The sparse index does not store every key. It stores one row per SSTable block:

```jsonc
{
  "firstKey": "customer:1000",
  "lastKey": "customer:1499",
  "offset": 348,
  "length": 553,
  "recordCount": 500,
  "checksum": "..."
}
```

For a point lookup that misses the memtable, the store first reads sparse-index
sidecars across the SSTable files and selects files whose block ranges can contain
the requested key. It then checks Bloom filters and opens only those candidate
data files. Since different flushes can have overlapping key ranges, all relevant
indexes are considered and the highest sequence wins. Legacy SSTables without an
index remain readable through the full-file fallback.

Range reads still merge records from all relevant SSTable files, keep the newest
sequence for each key, and discard tombstones.

### Compaction

Each flush creates a new tier-0 logical run. A logical run may contain multiple
physical SSTables when the file-size target is reached. When a tier reaches four
logical runs, compaction merges those runs, keeps the newest sequence for each
key, retains tombstones for safety, and writes one logical run into the next
tier. Output files are completed before input files are deleted, so a failed
compaction does not remove the prior readable runs. See [the tiered-compaction
design](./design/tiered-compaction.md) for the run format, promotion policy,
read path, and crash-safety rules.

```text
Flush 1 -> Tier 0: Run A
Flush 2 -> Tier 0: Run B
Flush 3 -> Tier 0: Run C
Flush 4 -> Tier 0: Run D

Tier 0: Run A + Run B + Run C + Run D
                         |
                         v
Tier 1: existing Run E + existing Run F + new Run G
                         |
              (when Tier 1 reaches four runs)
                         v
Tier 2: promoted Run H
```

Tier 1 does not replace its existing runs when a Tier-0 compaction completes;
the promoted run is added alongside them. If that makes four Tier-1 logical
runs, Tier 1 is compacted and promoted to Tier 2. Each logical run may contain
multiple physical SSTables, while reads merge all tiers by sequence.

## Write Path

1. API or SQL receives a `PUT`, `INSERT`, `UPDATE`, or `DELETE` for a table.
2. The database routes the operation to that table's LSM store.
3. The table store appends the operation to that table's write-ahead log.
4. The table store applies the operation to that table's ordered memtable.
5. The global change log records the committed table/key event.
6. If the table memtable reaches the flush threshold:
   - write that table's memtable as a new SSTable,
   - clear that table's write-ahead log,
   - compact eligible logical runs and promote the merged output to the next tier.

## Point Read Path

1. Route the read to the requested table.
2. Check that table's memtable first.
3. If the key is tombstoned, return not found.
4. Read sparse indexes to select candidate SSTable files whose block ranges can
   contain the key.
5. Use candidate Bloom filters to skip definite misses.
6. Seek to candidate block offsets and deserialize only those blocks.
7. Return the newest non-tombstoned value, or not found.

## Range Read Path

1. Route the range read to the requested table.
2. Read the sorted table memtable range.
3. Read matching sorted ranges from that table's SSTables.
4. Merge records by key.
5. Keep the newest sequence number for each key.
6. Drop tombstoned keys.
7. Return up to `limit` records in sorted key order.

Each table can retain multiple logical runs across Tier 0 and older promoted
tiers, with each run represented by one or more bounded SSTable files plus the
active memtable. Reads merge these files by sequence, while tiered compaction
promotes groups of four logical runs.

## Storage Layout

Runtime data lives under `data/`:

- `data/catalog.json`: table catalog.
- `data/indexes/catalog.json`: SQL JSON value index definitions.
- `data/indexes/{index}/metadata.json`: one JSON value index B+ tree root page,
  next page id, and order.
- `data/indexes/{index}/pages/page-*.json`: disk-backed B+ tree pages for that
  index.
- `data/sstable-indexes/catalog.json`: FASTWRITE exact-value index definitions.
- `data/sstable-indexes/{index}/`: hidden LSM/SSTable data for one FASTWRITE index.
- `data/changelog.log`: legacy durable change-log file, still read during upgrade.
- `data/changelog-*.log`: bounded numbered change-log segments; replay walks them in sequence order and the newest segment receives appends.
- `data/raft-state.json`: persisted Raft term and vote.
- `data/raft-replication.json`: last leader change sequence applied by a
  follower.
- `data/tables/{table}/wal.log`: pending direct writes and committed transaction
  batches for one table that are not yet flushed.
- `data/tables/{table}/sstables/*.json`: immutable sorted tables for one table.
- `data/tables/{table}/sstables/*.bloom.json`: Bloom filter sidecars for that
  table's SSTables.
- `data/tables/{table}/sstables/*.index.json`: sparse index sidecars with block
  key ranges and byte offsets for that table's SSTables.

SSTable block size can be configured in `appsettings.json`:

```json
{
  "Lsm": {
    "FlushThreshold": 100,
    "BlockSizeBytes": 16384,
    "MaxSstableFileSizeBytes": 67108864,
    "ChangeLogSegmentMaxBytes": 67108864
  }
}
```

## Testing

Unit tests live under `tests/LsmWriteDb.Tests`.

Run them with:

```bash
dotnet test .\tests\LsmWriteDb.Tests\LsmWriteDb.Tests.csproj
```

## Benchmarks

The repository includes a reproducible HTTP benchmark suite for all four data
paradigms in standalone and three-node distributed modes. It measures writes,
reads, range or predicate queries, full-text index building, search latency,
throughput, errors, p50/p95/p99 latency, and available Docker CPU/memory
snapshots. See [benchmarks/README.md](./benchmarks/README.md) for setup and
commands.

Use small runs while developing:

```powershell
python benchmarks/run.py --mode standalone --records 1000 --concurrency 8
python benchmarks/run.py --mode distributed --records 1000 --concurrency 8
```

## Docker

Build the image:

```bash
docker build -t heavy-write-db .
```

Run the container:

```bash
docker run --rm -p 8080:8080 -p 6543:6543 \
  -e TcpSql__Host=0.0.0.0 \
  heavy-write-db
```

Start a detached Docker container and open the interactive TCP SQL CLI:

```powershell
.\scripts\tcp-sql-cli.cmd
```

What the script does:

- Builds the Docker image automatically when it does not exist.
- Runs a detached container with HTTP on `http://localhost:8080` and TCP SQL on
  `127.0.0.1:6543`.
- Opens an interactive SQL prompt that supports multi-line statements ending
  with `;`.
- Renders `SELECT` results as a table in the console.
- Warns if you start a new SQL command before ending the previous command with
  `;`. Use `\clear` to discard an unfinished statement.
- Sends `QUIT;` and removes the container when you type `exit`, `quit`, or
  `\q`.

If Docker reports `dockerDesktopLinuxEngine` cannot be found, Docker Desktop is
not running or the Linux engine is still starting. Start Docker Desktop, wait for
the engine to be ready, then rerun the CLI command.

If the TCP connection closes unexpectedly, the script prints the last container
logs and leaves `lsm-write-db-tcp-sql` running for inspection.

Useful options:

```powershell
.\scripts\tcp-sql-cli.cmd -Rebuild
.\scripts\tcp-sql-cli.cmd -KeepContainer
.\scripts\tcp-sql-cli.cmd -TcpPort 7654 -HttpPort 9080
```

Example CLI session:

```text
sql> CREATE TABLE users;
sql> INSERT INTO users VALUES ('user:1001', '{"name":"Ada","tier":"gold"}');
sql> CREATE INDEX idx_users_tier ON users (value.tier);
sql> SELECT key, value FROM users WHERE value.tier = 'gold';
sql> exit
```

Run a local three-node Raft cluster:

```bash
docker compose up --build
```

The compose cluster exposes:

- `node-a` on `http://localhost:8081`
- `node-b` on `http://localhost:8082`
- `node-c` on `http://localhost:8083`

Each container advertises its internal Docker DNS URL, such as
`http://node-a:8080`, so Raft vote and heartbeat RPCs stay inside the compose
network. Use `/raft/state` on each host port to see which node is leader.

# flyokai/laminas-db-driver-async

> User docs → [`README.md`](README.md) · Agent quick-ref → [`CLAUDE.md`](CLAUDE.md) · Agent deep dive → [`AGENTS.md`](AGENTS.md)

Dual-strategy async database driver for Laminas DB: PDO via AMPHP worker pools and MySQLi via Revolt event loop.

## Architecture

Two independent async implementations sharing the Laminas DB adapter interface:

```
AsyncPdo  — AMPHP Parallel worker processes (process isolation)
AsyncMysqli — Revolt event loop + MYSQLI_ASYNC (in-process cooperative)
```

## AsyncPdo (Worker Pool Strategy)

### How It Works

1. `PdoWorkerPool` manages pool of AMPHP worker processes (default 32)
2. Each DB operation creates a `PdoTask` (serializable work unit)
3. Task sent to worker via `Worker::submit()`, executes in isolated process
4. Worker maintains `LocalCache` of PDO/PDOStatement objects keyed by `spl_object_id()`
5. Only scalar results cross process boundary — no PDO objects serialized

### Key Classes

**Connection** — extends Laminas PDO Connection:
- FiberLocal state: `transactionLevel`, `pdoResource`, `pdoDriverName`
- Transaction nesting with rollback tracking
- `fiber_mode` parameter (default true)

**PdoWorkerPool** — AMPHP worker pool manager:
- Default limit: 32 workers
- Worker selection: find worker with lowest connection count
- `PooledWorker` wraps worker with auto-return on destruct

**PdoTask** — serializable task dispatched to workers:
- Operations: `connect`, `execute`, `prepare`, `statement:execute`, `statement:fetch`, etc.
- Routes to cached PDO/PDOStatement in worker's LocalCache

**Statement** — prepared statement wrapper:
- Full prepared statement support (true server-side preparation)
- `PendingOperationError` thrown if concurrent ops on same resource

**Result** — with buffering support for re-iteration

### Execution Flow

```
Connection.execute() → PdoTask('execute', [sql])
  → PooledWorker.execute() → Worker.submit(task).await()
  → Worker-side: LocalCache[pdoId]->exec(sql) → serialize result back
```

## AsyncMysqli (Event Loop Strategy)

### How It Works

1. `LinkPool` manages queue of mysqli connections (default 32)
2. Query issued with `MYSQLI_ASYNC` flag (non-blocking)
3. `EventLoop::onMysqli()` registers callback for completion
4. Fiber suspends, yielding to event loop
5. On completion, `reap_async_query()` retrieves result, fiber resumes

### Key Classes

**Connection** — wraps `PooledLink`:
- Delegates to `ParentConnection` for actual mysqli operations
- `queryWithRetry()` — auto-reconnect on connection errors (2006, 2013), retry on deadlock (1213), max 10 retries

**LinkPool** — queue-based pool:
- Idle links in `SplQueue`, waiting requests as `DeferredFuture`
- `PooledLink` auto-returns to pool on destruct
- Factory creates `ParentConnection` instances

**Statement** — async execution:
```php
$this->getConnection()->query($sql, MYSQLI_ASYNC);
$suspension = EventLoop::getSuspension();
EventLoop::onMysqli($this->mysqli(), fn($id, $link) use ($suspension) {
    EventLoop::cancel($id);
    $suspension->resume($link->reap_async_query());
});
$result = $suspension->suspend();
```

**ParentConnection** — manages raw `mysqli` resource with retry logic

### Parameter Binding

No prepared statements — parameters interpolated client-side via `Platform::quoteValue()`. Named params sorted by length (longest first) to avoid partial replacement.

## Comparison

| Aspect | AsyncPdo | AsyncMysqli |
|--------|----------|-------------|
| Isolation | Strong (separate processes) | Weak (same process) |
| Overhead | Higher (serialization, IPC) | Lower (in-process) |
| Statements | True server-side prepared | Client-side interpolation |
| Scalability | CPU-bound tasks | I/O-bound tasks |
| Transactions | Full nesting via level tracking | Basic begin/commit/rollback |
| Fiber-aware | FiberLocal for state isolation | Implicit via event loop |

## Connection Pool Interface

`ConnectionPool` (interface) — factory contract used by Flyokai's `AmpConnectionPool`, `AsyncPdoConnectionPool`, `AsyncMysqliConnectionPool`.

`CreateSqlTrait` — helper for creating Laminas `Sql` builders from pool connections.

## Gotchas

- **PendingOperationError (PDO)**: Cannot start operation while one is in flight on same resource. Await before next operation.
- **FiberLocal isolation**: `fiber_mode=true` (default) isolates state per fiber. `fiber_mode=false` shares global state — race condition risk.
- **Process serialization (PDO)**: Only scalars and arrays cross worker boundary. Complex objects, closures, and resources cannot be serialized.
- **No prepared statements (MySQLi)**: Parameters interpolated client-side. Relies on `quoteValue()` for safety.
- **Pool starvation**: If all 32 workers/links occupied, subsequent requests block until one returns.
- **Retry hides failures (MySQLi)**: Auto-reconnect on error codes 2006/2013 and deadlock 1213. Max 10 retries before giving up.
- **Worker cache pollution (PDO)**: `LocalCache` in workers persists across executions. Long-running workers may leak memory.
- **Requires active EventLoop**: Both strategies need Revolt event loop context. Operations fail outside async context.

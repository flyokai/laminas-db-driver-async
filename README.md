# flyokai/laminas-db-driver-async

> User docs → [`README.md`](README.md) · Agent quick-ref → [`CLAUDE.md`](CLAUDE.md) · Agent deep dive → [`AGENTS.md`](AGENTS.md)

> Two-strategy async database driver for Laminas DB: **AsyncPdo** (AMPHP worker pools, process-isolated) and **AsyncMysqli** (Revolt event loop + `MYSQLI_ASYNC`, in-process cooperative).

Pick whichever fits your workload — both implement Laminas DB's `DriverInterface`. AsyncMysqli is the default `ConnectionPool` in Flyokai.

## Features

- **AsyncPdo** — strong process isolation via AMPHP `Parallel` workers (default 32). True server-side prepared statements.
- **AsyncMysqli** — in-process cooperative concurrency via `MYSQLI_ASYNC` and `EventLoop::onMysqli()`. Auto-retry on connection errors and deadlocks.
- **`ConnectionPool`** interface — factory contract used by Flyokai's `AmpConnectionPool`, `AsyncPdoConnectionPool`, `AsyncMysqliConnectionPool`
- **FiberLocal state** — transaction level / pdo resource isolated per fiber
- **`CreateSqlTrait`** — convenience for building Laminas `Sql` instances from pool connections

## Installation

```bash
composer require flyokai/laminas-db-driver-async
```

## When to use which

| Aspect | AsyncPdo | AsyncMysqli |
|--------|----------|-------------|
| Isolation | Strong (separate processes) | Weak (same process) |
| Overhead | Higher (serialization, IPC) | Lower (in-process) |
| Statements | True server-side prepared | Client-side parameter interpolation |
| Scalability | CPU-bound work | I/O-bound work |
| Transactions | Full nesting via level tracking | Basic begin/commit/rollback |
| Fiber-aware state | `FiberLocal` | Implicit via the event loop |

## AsyncPdo

### Architecture

```
PdoTask (serializable) ──► PooledWorker.execute()
                            ▼
                       Worker process
                       LocalCache[pdoId] → PDO/PDOStatement
                       returns scalar result
```

### Quick start

```php
use Flyokai\LaminasDbDriverAsync\AsyncPdo\PdoWorkerPool;
use Flyokai\LaminasDbDriverAsync\AsyncPdo\Connection;

$pool       = new PdoWorkerPool(workerLimit: 32);
$connection = new Connection($pool, [
    'dsn'      => 'mysql:host=localhost;dbname=app',
    'username' => 'app',
    'password' => 'secret',
]);

// Use $connection through Laminas DB exactly as you'd use a sync PDO connection.
```

`PdoTask` operations: `connect`, `execute`, `prepare`, `statement:execute`, `statement:fetch`, `statement:fetchAll`, … each routes to a cached PDO/PDOStatement in the worker via `spl_object_id()`.

## AsyncMysqli

### Architecture

```
$mysqli->query($sql, MYSQLI_ASYNC);
EventLoop::onMysqli($link, fn() => $suspension->resume($link->reap_async_query()));
$result = $suspension->suspend();
```

### Quick start

```php
use Flyokai\LaminasDbDriverAsync\AsyncMysqli\LinkPool;
use Flyokai\LaminasDbDriverAsync\AsyncMysqli\Connection;

$pool       = new LinkPool($factory, max: 32);
$connection = new Connection($pool);
```

The `Connection` delegates to `ParentConnection` for raw mysqli operations and adds `queryWithRetry()`:

- Automatic reconnect on errors `2006` (gone away) and `2013` (lost during query)
- Retry on deadlock `1213`
- Up to 10 retries

### Parameter binding

There are no prepared statements — parameters are interpolated client-side via `Platform::quoteValue()`. Named parameters are sorted by length (longest first) to avoid partial replacement.

## In Flyokai

`AsyncMysqliConnectionPool` is the **default** `ConnectionPool` configured via the DI alias in the application's `diconfig.php`. Switch to `AsyncPdoConnectionPool` or `AmpConnectionPool` by overriding the alias.

## Gotchas

- **`PendingOperationError` (PDO)** — cannot start a new operation while another is in flight on the same resource. Await before issuing the next call.
- **`fiber_mode=true`** isolates state per fiber. `fiber_mode=false` shares global state — race-condition risk.
- **Process serialization (PDO)** — only scalars and arrays cross worker boundaries. Closures, resources, complex objects don't.
- **No prepared statements (MySQLi)** — parameters are interpolated. Safety relies entirely on `quoteValue()`.
- **Pool starvation** — if all workers/links are occupied, subsequent requests block until one returns.
- **Auto-retry hides failures (MySQLi)** — connection errors and deadlocks are retried up to 10 times before giving up.
- **Worker cache pollution (PDO)** — `LocalCache` in workers persists across executions. Long-running workers may leak memory if you forget to release statements.
- **Requires active EventLoop** — both strategies need a Revolt event loop context.

## See also

- [`flyokai/laminas-db`](../laminas-db/README.md) — base abstraction
- [`flyokai/laminas-db-driver-amp`](../laminas-db-driver-amp/README.md) — alternative driver using `amphp/mysql`
- [`flyokai/revolt-event-loop`](../revolt-event-loop/README.md) — `onMysqli()` is the AsyncMysqli primitive
- [`flyokai/application`](../application/README.md) — wires the connection pools into DI

## License

MIT

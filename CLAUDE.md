# flyokai/laminas-db-driver-async

> User docs → [`README.md`](README.md) · Agent quick-ref → [`CLAUDE.md`](CLAUDE.md) · Agent deep dive → [`AGENTS.md`](AGENTS.md)

Dual-strategy async DB driver: AsyncPdo (AMPHP worker pools) and AsyncMysqli (Revolt event loop + MYSQLI_ASYNC).

See [AGENTS.md](AGENTS.md) for detailed documentation.

## Quick Reference

- **AsyncPdo**: Process-isolated via AMPHP Parallel workers (default 32). True prepared statements. FiberLocal state.
- **AsyncMysqli**: In-process via `MYSQLI_ASYNC` + `EventLoop::onMysqli()`. Client-side param interpolation. Auto-retry on connection errors/deadlocks.
- **Pool interface**: `ConnectionPool` — factory contract for Flyokai connection pool implementations
- **Default in Flyokai**: `AsyncMysqliConnectionPool` (configured via alias in diconfig.php)
- **Key rule**: Both require active Revolt event loop. Cannot operate outside async context.

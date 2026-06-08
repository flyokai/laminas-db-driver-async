<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncMysqli;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Parallel\Worker\Worker;
use function Amp\async;

class LinkPool
{
    public const DEFAULT_POOL_LIMIT = 32;
    private int $pendingCount = 0;

    protected \SplQueue $waiting;
    protected \SplQueue $idleLinks;
    protected DeferredCancellation $deferredCancellation;
    protected \Closure $push;

    /** @var int Maximum number of workers to use for open files. */
    private int $limit;

    /** @var \SplObjectStorage<Worker, int> Worker storage. */
    private \SplObjectStorage $linkStorage;

    /** @var Future Pending worker request */
    private Future $pendingLink;

    public function __construct(
        protected \Closure $linkFactory,
        int                $limit = self::DEFAULT_POOL_LIMIT
    ) {
        $this->limit = $limit;
        $this->linkStorage = new \SplObjectStorage();
        $this->idleLinks = $idleLinks = new \SplQueue();
        $this->waiting = $waiting = new \SplQueue();

        $this->deferredCancellation = new DeferredCancellation();

        $this->push = static function (ParentConnection $link) use ($waiting, $idleLinks): void {
            if ($waiting->isEmpty()) {
                $idleLinks->push($link);
            } else {
                $waiting->dequeue()->complete($link);
            }
        };
    }

    public function __destruct()
    {
        if ($this->isRunning()) {
            $this->deferredCancellation->cancel();
        }
    }

    public function isRunning(): bool
    {
        return !$this->deferredCancellation->isCancelled();
    }

    public function isIdle(): bool
    {
        return $this->idleLinks->count() > 0 || $this->linkStorage->count() < $this->limit;
    }

    /**
     * @return int
     */
    public function getLimit(): int
    {
        return $this->limit;
    }

    public function getLinksCount(): int
    {
        return $this->linkStorage->count() + $this->pendingCount;
    }

    public function getIdleLinksCount(): int
    {
        return $this->idleLinks->count();
    }

    /** @var array<int, array{acquired_at: float, fiber_id: int, trace: string}> */
    private array $acquired = [];
    private int $serial = 0;

    public function getLink(): PooledLink
    {
        $link = $this->pull();
        $serial = ++$this->serial;
        $acquiredAt = microtime(true);
        $fiberId = \Fiber::getCurrent() !== null ? spl_object_id(\Fiber::getCurrent()) : 0;
        $trace = $this->formatTrace(debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 16));
        $acquired = &$this->acquired;
        $onRelease = static function () use (&$acquired, $serial): void {
            unset($acquired[$serial]);
        };
        $this->acquired[$serial] = [
            'acquired_at' => $acquiredAt,
            'fiber_id' => $fiberId,
            'trace' => $trace,
        ];
        return new PooledLink($link, $this->push, $onRelease);
    }

    private function formatTrace(array $frames): string
    {
        $out = [];
        foreach ($frames as $f) {
            $func = ($f['class'] ?? '') . ($f['type'] ?? '') . ($f['function'] ?? '');
            $loc = isset($f['file']) ? basename($f['file']) . ':' . ($f['line'] ?? '?') : '<internal>';
            $out[] = $func . ' (' . $loc . ')';
        }
        return implode(' <- ', $out);
    }

    /**
     * Debug snapshot — used by the on-demand pool-state HTTP endpoint and by the slow-wait logger.
     * @return array<string, int>
     */
    public function snapshot(): array
    {
        return [
            'limit' => $this->limit,
            'in_use' => $this->linkStorage->count(),
            'pending_create' => $this->pendingCount,
            'idle' => $this->idleLinks->count(),
            'waiting' => $this->waiting->count(),
            'tracked_acquires' => count($this->acquired),
        ];
    }

    /**
     * Per-acquire snapshot — returns one entry per currently-held link, with the call site
     * that acquired it and how long it's been held.
     *
     * @return array<int, array{age_s: float, fiber_id: int, trace: string}>
     */
    public function acquireTrace(): array
    {
        $now = microtime(true);
        $out = [];
        foreach ($this->acquired as $serial => $entry) {
            $out[$serial] = [
                'age_s' => round($now - $entry['acquired_at'], 3),
                'fiber_id' => $entry['fiber_id'],
                'trace' => $entry['trace'],
            ];
        }
        // Oldest first — those are the leak suspects.
        uasort($out, static fn($a, $b) => $b['age_s'] <=> $a['age_s']);
        return $out;
    }

    /**
     * @throws \RuntimeException
     */
    private function pull(): ParentConnection
    {
        if (!$this->isRunning()) {
            throw new \RuntimeException("The pool was shut down");
        }

        do {
            if ($this->idleLinks->isEmpty()) {
                /** @var DeferredFuture<MysqliConnection|null> $deferredFuture */
                $deferredFuture = new DeferredFuture;
                $this->waiting->enqueue($deferredFuture);

                // Debug instrumentation — flag suspensions when the pool is at limit, so we can
                // tell whether the periodic OAuth hang is correlated with pool saturation.
                $waitingBecauseSaturated = $this->getLinksCount() >= $this->limit;
                $fiberId = \Fiber::getCurrent() !== null ? spl_object_id(\Fiber::getCurrent()) : 0;
                $waitStartedAt = microtime(true);
                $slowTimer = null;
                if ($waitingBecauseSaturated) {
                    error_log(sprintf(
                        '[link-pool] WAIT_SATURATED fiber=%d pid=%d snapshot=%s',
                        $fiberId,
                        getmypid(),
                        json_encode($this->snapshot()),
                    ));
                    // Dump the full acquire trace ONCE the FIRST time we saturate (per pool
                    // instance) — that's the moment we want to know which call sites are
                    // holding the leaked links.
                    static $traceDumped = false;
                    if (!$traceDumped) {
                        $traceDumped = true;
                        foreach ($this->acquireTrace() as $serial => $entry) {
                            error_log(sprintf(
                                '[link-pool] LEAK_TRACE serial=%d age=%.1fs fiber=%d trace=%s',
                                $serial,
                                $entry['age_s'],
                                $entry['fiber_id'],
                                $entry['trace'],
                            ));
                        }
                    }
                    $snapshotFn = $this->snapshot(...);
                    $slowTimer = \Revolt\EventLoop::delay(2.0, static function () use ($fiberId, $waitStartedAt, $snapshotFn): void {
                        error_log(sprintf(
                            '[link-pool] SLOW_WAIT fiber=%d elapsed=%.2fs snapshot=%s',
                            $fiberId,
                            microtime(true) - $waitStartedAt,
                            json_encode($snapshotFn()),
                        ));
                    });
                }

                if ($this->getLinksCount() < $this->limit) {
                    // Max ObjectManager count has not been reached, so create another.
                    $this->pendingCount++;

                    $factory = $this->linkFactory;
                    $pending = &$this->pendingCount;
                    $cancellation = $this->deferredCancellation->getCancellation();
                    $linkStorage = $this->linkStorage;
                    $future = async(static function () use (&$pending, $factory, $linkStorage, $cancellation): ParentConnection {
                        try {
                            $link = $factory($cancellation);
                        } catch (CancelledException) {
                            throw new \RuntimeException('The pool shut down before the task could be executed');
                        } finally {
                            $pending--;
                        }

                        $linkStorage->attach($link, 0);
                        return $link;
                    });

                    $future
                        ->map($this->push)
                        ->catch(static function (\Throwable $e) use ($deferredFuture): void {
                            // Fail this one waiter only. Do NOT cancel the pool's
                            // deferredCancellation — a single factory failure (transient
                            // MySQL unavailability, momentary worker-spawn hiccup, etc.)
                            // must not poison the whole pool for the lifetime of the
                            // PHP process. Subsequent getLink() calls retry cleanly.
                            $deferredFuture->error($e);
                        })
                        ->ignore()
                    ;
                }

                $link = $deferredFuture->getFuture()->await();
                if ($slowTimer !== null) {
                    \Revolt\EventLoop::cancel($slowTimer);
                }
            } else {
                $link = $this->idleLinks->shift();
            }

            if ($link === null) {
                continue;
            }

            return $link;

            \assert($link instanceof ParentConnection);

            $this->linkStorage->detach($link);
        } while (true);
    }
}

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

    public function getLink(): PooledLink
    {
        return new PooledLink($this->pull(), $this->push);
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

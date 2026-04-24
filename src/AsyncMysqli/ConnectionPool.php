<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncMysqli;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Parallel\Worker\Worker;
use function Amp\async;

class ConnectionPool
{
    public const DEFAULT_POOL_LIMIT = 32;
    private int $pendingCount = 0;

    protected \SplQueue $waiting;
    protected \SplQueue $idleConnections;
    protected DeferredCancellation $deferredCancellation;
    protected \Closure $push;

    /** @var int Maximum number of workers to use for open files. */
    private int $limit;

    /** @var \SplObjectStorage<Worker, int> Worker storage. */
    private \SplObjectStorage $connectionStorage;

    /** @var Future Pending worker request */
    private Future $pendingConnection;

    public function __construct(
        protected \Closure $connectionFactory,
        int                $limit = self::DEFAULT_POOL_LIMIT
    ) {
        $this->limit = $limit;
        $this->connectionStorage = new \SplObjectStorage();
        $this->idleConnections = $idleConnections = new \SplQueue();
        $this->waiting = $waiting = new \SplQueue();

        $this->deferredCancellation = new DeferredCancellation();

        $this->push = static function (ParentConnection $connection) use ($waiting, $idleConnections): void {
            if ($waiting->isEmpty()) {
                $idleConnections->push($connection);
            } else {
                $waiting->dequeue()->complete($connection);
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
        return $this->idleConnections->count() > 0 || $this->connectionStorage->count() < $this->limit;
    }

    /**
     * @return int
     */
    public function getLimit(): int
    {
        return $this->limit;
    }

    public function getConnectionsCount(): int
    {
        return $this->connectionStorage->count() + $this->pendingCount;
    }

    public function getIdleConnectionsCount(): int
    {
        return $this->idleConnections->count();
    }

    public function getConnection(): PooledConnection
    {
        return new PooledConnection($this->pull(), $this->push);
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
            if ($this->idleConnections->isEmpty()) {
                /** @var DeferredFuture<MysqliConnection|null> $deferredFuture */
                $deferredFuture = new DeferredFuture;
                $this->waiting->enqueue($deferredFuture);

                if ($this->getConnectionsCount() < $this->limit) {
                    // Max ObjectManager count has not been reached, so create another.
                    $this->pendingCount++;

                    $factory = $this->connectionFactory;
                    $pending = &$this->pendingCount;
                    $cancellation = $this->deferredCancellation->getCancellation();
                    $connectionStorage = $this->connectionStorage;
                    $future = async(
                        static function ()
                            use (&$pending, $factory, $connectionStorage, $cancellation)
                            : ParentConnection
                    {
                        try {
                            $connection = $factory($cancellation);
                        } catch (CancelledException) {
                            throw new \RuntimeException('The pool shut down before the task could be executed');
                        } finally {
                            $pending--;
                        }

                        $connectionStorage->attach($connection, 0);
                        return $connection;
                    });

                    $waiting = $this->waiting;
                    $deferredCancellation = $this->deferredCancellation;
                    $future
                        ->map($this->push)
                        ->catch(static function (\Throwable $e) use ($deferredCancellation, $connectionStorage, $deferredFuture): void {
                            $deferredCancellation->cancel();
                            $deferredFuture->error($e);
                        })
                        ->ignore()
                    ;
                }

                $connection = $deferredFuture->getFuture()->await();
            } else {
                $connection = $this->idleConnections->shift();
            }

            if ($connection === null) {
                continue;
            }

            return $connection;

            \assert($connection instanceof ParentConnection);

            $this->connectionStorage->detach($connection);
        } while (true);
    }
}

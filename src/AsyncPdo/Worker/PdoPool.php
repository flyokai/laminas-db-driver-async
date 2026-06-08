<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo\Worker;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Future;
use function Amp\async;

class PdoPool
{
    use ForbidCloning;
    use ForbidSerialization;

    public const DEFAULT_LIMIT = 32;

    private int $pendingCount = 0;

    /** @var \SplObjectStorage<\PDO, int> */
    private readonly \SplObjectStorage $storage;

    /** @var \SplQueue<\PDO> */
    private readonly \SplQueue $idleQueue;

    /** @var \SplQueue<DeferredFuture<\PDO|null>> */
    private readonly \SplQueue $waiting;

    /** @var \Closure(\PDO):void */
    private readonly \Closure $push;

    private ?Future $exitStatus = null;

    private readonly DeferredCancellation $deferredCancellation;

    public function __construct(
        private readonly int $limit = self::DEFAULT_LIMIT,
    ) {
        if ($limit <= 0) {
            throw new \ValueError("PdoPool maximum size must be a positive integer");
        }
        $this->storage = new \SplObjectStorage();
        $this->idleQueue = $idleQueue = new \SplQueue();
        $this->waiting = $waiting = new \SplQueue();

        $this->deferredCancellation = new DeferredCancellation();

        $this->push = static function (\PDO $pdo) use ($waiting, $idleQueue): void {
            if ($waiting->isEmpty()) {
                $idleQueue->push($pdo);
            } else {
                $waiting->dequeue()->complete($pdo);
            }
        };
    }

    public function __destruct()
    {
        if ($this->isRunning()) {
            $this->deferredCancellation->cancel();
        }
    }

    /**
     * @return bool
     */
    public function isRunning(): bool
    {
        return !$this->deferredCancellation->isCancelled();
    }

    /**
     * @return bool
     */
    public function isIdle(): bool
    {
        return $this->idleQueue->count() > 0 || $this->storage->count() < $this->limit;
    }

    /**
     * @return int
     */
    public function getLimit(): int
    {
        return $this->limit;
    }

    public function getCount(): int
    {
        return $this->storage->count() + $this->pendingCount;
    }

    public function getIdleCount(): int
    {
        return $this->idleQueue->count();
    }

    public function getPdo($params): PooledPdo
    {
        $this->assertConnectionParams($params);
        return new PooledPdo($this->pull($params), $this->push);
    }

    protected function assertConnectionParams(array $params, bool $throw = true): bool
    {
        if (count($params)<1 || count($params)>4) {
            if ($throw) {
                throw new \Error(sprintf(
                    "PdoPool invalid number of connection params: %s", count($params)
                ));
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * @throws \Error
     */
    private function pull($params): \PDO
    {
        if (!$this->isRunning()) {
            throw new \Error("The PdoPool was shut down");
        }

        do {
            if ($this->idleQueue->isEmpty()) {
                /** @var DeferredFuture<\PDO|null> $deferredFuture */
                $deferredFuture = new DeferredFuture;
                $this->waiting->enqueue($deferredFuture);

                if ($this->getCount() < $this->limit) {
                    // Max PDO count has not been reached, so create another.
                    $this->pendingCount++;

                    $pending = &$this->pendingCount;
                    $cancellation = $this->deferredCancellation->getCancellation();
                    $storage = $this->storage;
                    $future = async(static function () use (&$pending, $storage, $cancellation, $params): \PDO {
                        try {
                            $connectorOptions = $params[3]['connectorOptions'] ?? [];
                            unset($params[3]['connectorOptions']);
                            $pdo = new \PDO(...$params);
                            $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
                            $initStatements = $connectorOptions['initStatements'] ?? [];
                            foreach ($initStatements as $statement) {
                                $pdo->query($statement);
                            }
                        } catch (CancelledException) {
                            throw new \Error('The PdoPool shut down before the task could be executed');
                        } finally {
                            $pending--;
                        }

                        $storage->attach($pdo, 0);
                        return $pdo;
                    });

                    $waiting = $this->waiting;
                    $deferredCancellation = $this->deferredCancellation;
                    $future
                        ->map($this->push)
                        ->catch(static function (\Throwable $e) use ($deferredCancellation, $storage, $deferredFuture): void {
                            $deferredCancellation->cancel();
                            $deferredFuture->error($e);
                        })
                        ->ignore()
                        ;
                }

                $pdo = $deferredFuture->getFuture()->await();
            } else {
                $pdo = $this->idleQueue->shift();
            }

            if ($pdo === null) {
                continue;
            }

            return $pdo;

            \assert($pdo instanceof \PDO);

            $this->storage->detach($pdo);
        } while (true);
    }
}

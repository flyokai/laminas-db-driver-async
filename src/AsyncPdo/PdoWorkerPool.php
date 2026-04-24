<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo;

use Amp\Future;
use Amp\Parallel\Worker\Worker;
use Amp\Parallel\Worker\WorkerPool;
use function Amp\async;
use function Amp\Parallel\Worker\workerPool;
use function Flyokai\LaminasDbDriverAsync\createSyncWorkerPool;
use function Flyokai\LaminasDbDriverAsync\syncWorkerPool;

class PdoWorkerPool
{
    public const DEFAULT_WORKER_LIMIT = 32;

    private WorkerPool $pool;

    /** @var int Maximum number of workers to use for open files. */
    private int $workerLimit;

    /** @var \SplObjectStorage<Worker, int> Worker storage. */
    private \SplObjectStorage $workerStorage;

    /** @var Future Pending worker request */
    private Future $pendingWorker;

    public function __construct(
        ?WorkerPool $pool = null,
        int $workerLimit = self::DEFAULT_WORKER_LIMIT
    ) {
        $this->pool = $pool ?? createSyncWorkerPool($workerLimit);
        $this->workerLimit = $workerLimit;
        $this->workerStorage = new \SplObjectStorage();
        $this->pendingWorker = Future::complete();
    }

    public function createPooledWorker(): PooledWorker
    {
        $worker = $this->selectWorker();

        $workerStorage = $this->workerStorage;
        return new PooledWorker($worker, static function (Worker $worker) use ($workerStorage): void {
            if (!$workerStorage->contains($worker)) {
                return;
            }

            if (($workerStorage[$worker] -= 1) === 0 || !$worker->isRunning()) {
                $workerStorage->detach($worker);
            }
        });
    }

    private function selectWorker(): Worker
    {
        $this->pendingWorker->await(); // Wait for any currently pending request for a worker.

        if ($this->workerStorage->count() < $this->workerLimit) {
            $this->pendingWorker = async($this->pool->getWorker(...));
            $worker = $this->pendingWorker->await();

            if ($this->workerStorage->contains($worker)) {
                // amphp/parallel v1.x may return an already used worker from the pool.
                $this->workerStorage[$worker] += 1;
            } else {
                // amphp/parallel v2.x should always return an unused worker.
                $this->workerStorage->attach($worker, 1);
            }

            return $worker;
        }

        $max = \PHP_INT_MAX;
        foreach ($this->workerStorage as $storedWorker) {
            $count = $this->workerStorage[$storedWorker];
            if ($count <= $max) {
                $worker = $storedWorker;
                $max = $count;
            }
        }

        \assert(isset($worker) && $worker instanceof Worker);

        if (!$worker->isRunning()) {
            $this->workerStorage->detach($worker);
            return $this->selectWorker();
        }

        $this->workerStorage[$worker] += 1;

        return $worker;
    }
}

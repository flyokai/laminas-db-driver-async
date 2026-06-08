<?php

namespace Flyokai\LaminasDbDriverAsync;

use Amp\Parallel\Worker\ContextWorkerPool;
use Amp\Parallel\Worker\WorkerPool;
use Revolt\EventLoop;
use Flyokai\LaminasDbDriverAsync\SyncWorker\SyncWorkerFactory;

function syncWorkerPool(?WorkerPool $pool = null): WorkerPool
{
    static $map;
    $map ??= new \WeakMap();
    $driver = EventLoop::getDriver();

    if ($pool) {
        return $map[$driver] = $pool;
    }

    return $map[$driver] ??= new ContextWorkerPool(WorkerPool::DEFAULT_WORKER_LIMIT, new SyncWorkerFactory);
}

function createSyncWorkerPool(int $workerLimit = WorkerPool::DEFAULT_WORKER_LIMIT)
{
    return new ContextWorkerPool($workerLimit, new SyncWorkerFactory);
}



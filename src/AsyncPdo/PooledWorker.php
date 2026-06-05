<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo;

use Amp\Cancellation;
use Amp\Parallel\Worker\Task;
use Amp\Parallel\Worker\Worker;

class PooledWorker
{
    /**
     * @param \Closure(Worker):void $push Closure to push the worker back into the queue.
     */
    public function __construct(
        private readonly Worker $worker,
        private readonly \Closure $push
    ) {
    }

    /**
     * Automatically pushes the worker back into the queue.
     */
    public function __destruct()
    {
        ($this->push)($this->worker);
    }

    public function isRunning(): bool
    {
        return $this->worker->isRunning();
    }

    public function isIdle(): bool
    {
        return $this->worker->isIdle();
    }

    public function execute(PdoTask $task, ?Cancellation $cancellation = null): mixed
    {
        return $this->worker->submit($task, $cancellation)->await();
    }

    public function shutdown(): void
    {
        $this->worker->shutdown();
    }

    public function kill(): void
    {
        $this->worker->kill();
    }
}
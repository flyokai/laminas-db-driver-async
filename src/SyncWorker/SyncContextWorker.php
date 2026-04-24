<?php

namespace Flyokai\LaminasDbDriverAsync\SyncWorker;

use Amp\ByteStream\ReadableBuffer;
use Amp\ByteStream\StreamChannel;
use Amp\ByteStream\WritableBuffer;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Future;
use Amp\Parallel\Context\Context;
use Amp\Parallel\Context\StatusError;
use Amp\Parallel\Worker\Execution;
use Amp\Parallel\Worker\Task;
use Amp\Parallel\Worker\Worker;
use Amp\Parallel\Worker\WorkerException;
use Amp\Sync\ChannelException;
use Amp\Sync\LocalMutex;
use Amp\Sync\Mutex;
use Amp\TimeoutCancellation;
use Revolt\EventLoop;
use function Amp\async;
use Amp\Parallel\Worker\Internal;
use function Amp\Sync\synchronized;

class SyncContextWorker implements Worker
{
    public static $prevMem = 0;
    public static $dump = false;
    public static $count = 0;
    use ForbidCloning;
    use ForbidSerialization;

    private const SHUTDOWN_TIMEOUT = 1;
    private const ERROR_TIMEOUT = 0.25;

    private readonly \Closure $onReceive;

    private ?Future $exitStatus = null;

    private ?DeferredFuture $result = null;

    public function __destruct()
    {
        self::$count--;
    }

    /**
     * @param Context<int, Internal\JobPacket, Internal\JobPacket|null> $context A context running tasks.
     */
    public function __construct(
        private readonly Context $context,
        protected Mutex $mutex = new LocalMutex,
    ) {
        self::$count++;
    }

    private function onReceive(?\Throwable $exception, ?Internal\JobPacket $data): void
    {
        if (!$data || !$this->result) {
            $exception ??= new WorkerException("Unexpected error in worker");
            $this->result?->error($exception);
            $this->context->close();
            return;
        }

        $id = $data->getId();

        try {

            if (!$data instanceof Internal\TaskResult) {
                $resultType = gettype($data);
                if ($resultType === 'object') {
                    $resultType = get_class($data);
                }
                $this->result->error(new WorkerException(sprintf(
                    "Wrong result in worker: expected TaskResult, got %s instead.", $resultType
                )));
                return;
            }

            try {
                $this->result->complete($data->getResult());
            } catch (\Throwable $exception) {
                $this->result->error($exception);
            }
        } finally {
        }
    }

    private static function receive(Context $context, callable $onReceive): void
    {
        EventLoop::queue(static function () use ($context, $onReceive): void {
            try {
                $received = $context->receive();
            } catch (\Throwable $exception) {
                $onReceive($exception, null);
                return;
            }

            $onReceive(null, $received);
        });
    }

    public function isRunning(): bool
    {
        // Report as running unless shutdown or killed.
        return $this->exitStatus === null;
    }

    public function isIdle(): bool
    {
        return empty($this->jobQueue);
    }

    public function submit(Task $task, ?Cancellation $cancellation = null): Execution
    {
        return synchronized($this->mutex, $this->_submit(...),  $task, $cancellation);
    }

    protected function _submit(Task $task, ?Cancellation $cancellation = null): Execution
    {
        if ($this->exitStatus) {
            throw new StatusError("The worker has been shut down");
        }

        try {
            $cancellation?->throwIfRequested();
        } catch (CancelledException $exception) {
            return self::createCancelledExecution($task, $exception);
        }

        $this->result = $deferred = new DeferredFuture;
        $future = $deferred->getFuture();

        try {
            $this->context->send($task);
            self::receive($this->context, $this->onReceive(...));
        } catch (ChannelException $exception) {
            try {
                $exception = new WorkerException("The worker exited unexpectedly", 0, $exception);
                $this->context->join(new TimeoutCancellation(self::ERROR_TIMEOUT));
            } catch (CancelledException) {
                $this->kill();
            } catch (\Throwable $exception) {
                $exception = new WorkerException("The worker crashed", 0, $exception);
            }

            $this->exitStatus ??= Future::error($exception);

            unset($this->result);
            throw $exception;
        } catch (\Throwable $exception) {
            unset($this->result);
            throw $exception;
        }

        $future->await();

        /** @psalm-suppress InvalidArgument */
        return new Execution($task, $this->context, $future);
    }

    public function shutdown(): void
    {
        if ($this->exitStatus) {
            $this->exitStatus->await();
            return;
        }

        ($this->exitStatus = async(function (): void {
            if ($this->context->isClosed()) {
                throw new WorkerException("The worker had crashed prior to being shutdown");
            }

            // Wait for pending tasks to finish.
            $this->result->getFuture()->await();

            try {
                $this->context->send(null); // End loop in task runner.
                $this->context->join(new TimeoutCancellation(self::SHUTDOWN_TIMEOUT));
            } catch (\Throwable $exception) {
                $this->context->close();
                throw new WorkerException("Failed to gracefully shutdown worker", 0, $exception);
            }
        }))->await();
    }

    public function kill(): void
    {
        if (!$this->context->isClosed()) {
            $this->context->close();
        }

        $this->exitStatus ??= Future::error(new WorkerException("The worker was killed"));
        $this->exitStatus->ignore();
    }

    private static function createCancelledExecution(Task $task, CancelledException $exception): Execution
    {
        $channel = new StreamChannel(new ReadableBuffer(), new WritableBuffer());
        $channel->close();

        return new Execution($task, $channel, Future::error($exception));
    }
}

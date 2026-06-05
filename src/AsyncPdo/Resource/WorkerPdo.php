<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo\Resource;

use Amp\ByteStream\StreamException;
use Amp\Cancellation;
use Amp\Parallel\Worker\TaskFailureException;
use Amp\Parallel\Worker\WorkerException;
use Revolt\EventLoop;
use Flyokai\LaminasDbDriverAsync\AsyncPdo\PdoTask;
use Flyokai\LaminasDbDriverAsync\AsyncPdo\PooledWorker;
use Flyokai\LaminasDbDriverAsync\AsyncPdo\PendingOperationError;

class WorkerPdo implements Pdo
{
    protected bool $busy = false;

    public function __construct(
        public readonly string $id,
        public readonly PooledWorker $worker
    ) {
    }

    public function prepare(string $query, array $options = []): Statement|false
    {
        $stmtId = $this->runTask(new PdoTask(
            'pdo:prepare',
            [$query, $options], $this->id
        ));
        return new WorkerStatement($stmtId, $this);
    }

    public function beginTransaction(): bool
    {
        return $this->runTask(new PdoTask(
            'pdo:beginTransaction',
            [], $this->id
        ));
    }

    public function commit(): bool
    {
        return $this->runTask(new PdoTask(
            'pdo:commit',
            [], $this->id
        ));
    }

    public function rollBack(): bool
    {
        return $this->runTask(new PdoTask(
            'pdo:rollBack',
            [], $this->id
        ));
    }

    public function inTransaction(): bool
    {
        return $this->runTask(new PdoTask(
            'pdo:inTransaction',
            [], $this->id
        ));
    }

    public function setAttribute(int $attribute, mixed $value): bool
    {
        $this->runTask(new PdoTask(
            'pdo:setAttribute',
            [$attribute, $value], $this->id
        ));
    }

    public function exec(string $statement): int|false
    {
        return $this->runTask(new PdoTask(
            'pdo:exec',
            [$statement], $this->id
        ));
    }

    public function query(string $query, ?int $fetchMode = null, ...$fetch_mode_args): Statement|false
    {
        $stmtId = $this->runTask(new PdoTask(
            'pdo:query',
            [$query, $fetchMode, ...$fetch_mode_args], $this->id
        ));
        return new WorkerStatement($stmtId, $this);
    }

    public function lastInsertId($name = null): string|false
    {
        return $this->runTask(new PdoTask(
            'pdo:lastInsertId',
            [$name], $this->id
        ));
    }

    public function errorCode(): ?string
    {
        return $this->runTask(new PdoTask(
            'pdo:errorCode',
            [], $this->id
        ));
    }

    public function errorInfo(): array
    {
        return $this->runTask(new PdoTask(
            'pdo:errorInfo',
            [], $this->id
        ));
    }

    public function getAttribute(int $attribute): mixed
    {
        return $this->runTask(new PdoTask(
            'pdo:getAttribute',
            [$attribute], $this->id
        ));
    }

    public function quote(string $string, int $type = \PDO::PARAM_STR): string|false
    {
        return $this->runTask(new PdoTask(
            'pdo:quote',
            [$string, $type], $this->id
        ));
    }


    public function __destruct()
    {
        $id = $this->id;
        $worker = $this->worker;
        EventLoop::queue(static fn () => $worker->execute(new PdoTask('pdo:destroy', [], $id)));
    }

    private function runTask(PdoTask $task, ?Cancellation $cancellation = null)
    {
        if ($this->busy) {
            throw new PendingOperationError;
        }

        $this->busy = true;
        try {
            $data = $this->worker->execute($task, $cancellation);
        } catch (TaskFailureException $exception) {
            throw new StreamException("Reading from the worker failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new StreamException("Sending the task to the worker failed", 0, $exception);
        } finally {
            $this->busy = false;
        }

        return $data;
    }
}
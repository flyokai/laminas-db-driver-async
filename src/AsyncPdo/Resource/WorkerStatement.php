<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo\Resource;

use Amp\ByteStream\StreamException;
use Amp\Cancellation;
use Amp\Parallel\Worker\TaskFailureException;
use Amp\Parallel\Worker\Worker;
use Amp\Parallel\Worker\WorkerException;
use Revolt\EventLoop;
use Flyokai\LaminasDbDriverAsync\AsyncPdo\PdoTask;
use Flyokai\LaminasDbDriverAsync\AsyncPdo\PendingOperationError;

class WorkerStatement implements Statement
{
    protected bool $busy = false;

    public function __construct(
        public readonly string $id,
        public readonly WorkerPdo $pdo
    ) {
    }

    public function __destruct()
    {
        $id = $this->id;
        $worker = $this->pdo->worker;
        EventLoop::queue(static fn () => $worker->execute(new PdoTask('statement:destroy', [], $id)));
    }

    public function execute($params = null): bool
    {
        return $this->runTask(new PdoTask(
            'statement:execute',
            [$params], $this->id
        ));
    }

    public function fetch($mode = \PDO::FETCH_DEFAULT, $cursorOrientation = \PDO::FETCH_ORI_NEXT, $cursorOffset = 0): mixed
    {
        return $this->runTask(new PdoTask(
            'statement:fetch',
            [$mode, $cursorOrientation, $cursorOffset], $this->id
        ));
    }

    public function bindParam($param, $var, $type = \PDO::PARAM_STR, $maxLength = 0, $driverOptions = null): bool
    {
        return $this->runTask(new PdoTask(
            'statement:bindParam',
            [$param, $var, $type, $maxLength, $driverOptions], $this->id
        ));
    }

    public function bindParams($params): bool
    {
        return $this->runTask(new PdoTask(
            'statement:bindParams',
            $params, $this->id
        ));
    }

    public function bindColumn($column, $var, $type = \PDO::PARAM_STR, $maxLength = 0, $driverOptions = null): bool
    {
        return $this->runTask(new PdoTask(
            'statement:bindColumn',
            [$column, $var, $type, $maxLength, $driverOptions], $this->id
        ));
    }

    public function bindValue($param, $value, $type = \PDO::PARAM_STR): bool
    {
        return $this->runTask(new PdoTask(
            'statement:bindValue',
            [$param, $value, $type], $this->id
        ));
    }

    public function rowCount(): int
    {
        return $this->runTask(new PdoTask(
            'statement:rowCount',
            [], $this->id
        ));
    }

    public function fetchColumn($column = 0): mixed
    {
        return $this->runTask(new PdoTask(
            'statement:fetchColumn',
            [$column], $this->id
        ));
    }

    public function fetchAll($mode = \PDO::FETCH_DEFAULT, ...$args): array
    {
        return $this->runTask(new PdoTask(
            'statement:fetchAll',
            [$mode , ...$args], $this->id
        ));
    }

    public function fetchObject($class = "stdClass", $constructorArgs = []): object|false
    {
        return $this->runTask(new PdoTask(
            'statement:fetchObject',
            [$class, $constructorArgs], $this->id
        ));
    }

    public function errorCode(): ?string
    {
        return $this->runTask(new PdoTask(
            'statement:errorCode',
            [], $this->id
        ));
    }

    public function errorInfo(): array
    {
        return $this->runTask(new PdoTask(
            'statement:errorInfo',
            [], $this->id
        ));
    }

    public function setAttribute($attribute, $value): bool
    {
        return $this->runTask(new PdoTask(
            'statement:setAttribute',
            [$attribute, $value], $this->id
        ));
    }

    public function getAttribute($name): mixed
    {
        return $this->runTask(new PdoTask(
            'statement:getAttribute',
            [$name], $this->id
        ));
    }

    public function columnCount(): int
    {
        return $this->runTask(new PdoTask(
            'statement:columnCount',
            [], $this->id
        ));
    }

    public function getColumnMeta($column): array|false
    {
        return $this->runTask(new PdoTask(
            'statement:getColumnMeta',
            [$column], $this->id
        ));
    }

    public function setFetchMode(...$params): bool
    {
        return $this->runTask(new PdoTask(
            'statement:setFetchMode',
            [...$params], $this->id
        ));
    }

    public function nextRowset(): bool
    {
        return $this->runTask(new PdoTask(
            'statement:nextRowset',
            [], $this->id
        ));
    }

    public function closeCursor(): bool
    {
        return $this->runTask(new PdoTask(
            'statement:closeCursor',
            [], $this->id
        ));
    }


    private function runTask(PdoTask $task, ?Cancellation $cancellation = null)
    {
        if ($this->busy) {
            throw new PendingOperationError;
        }

        $this->busy = true;
        try {
            $data = $this->pdo->worker->execute($task, $cancellation);
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
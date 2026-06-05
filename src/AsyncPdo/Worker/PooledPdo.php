<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo\Worker;

use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Revolt\EventLoop;

class PooledPdo
{
    use ForbidCloning;
    use ForbidSerialization;

    public function __construct(
        public readonly \PDO $pdo,
        private readonly \Closure $push,
    ) {
    }

    public function __destruct()
    {
        $push = $this->push;
        $pdo = $this->pdo;
        EventLoop::queue(static fn() => ($push)($pdo));
    }

    public function prepare(...$args)
    {
        return $this->pdo->prepare(...$args);
    }
    public function beginTransaction()
    {
        return $this->pdo->beginTransaction();
    }
    public function commit()
    {
        return $this->pdo->commit();
    }
    public function rollBack()
    {
        return $this->pdo->rollBack();
    }
    public function inTransaction()
    {
        return $this->pdo->inTransaction();
    }
    public function setAttribute(...$args)
    {
        return $this->pdo->setAttribute(...$args);
    }
    public function exec(...$args)
    {
        return $this->pdo->exec(...$args);
    }
    public function query(...$args)
    {
        return $this->pdo->query(...$args);
    }
    public function lastInsertId(...$args)
    {
        return $this->pdo->lastInsertId(...$args);
    }
    public function errorCode()
    {
        return $this->pdo->errorCode();
    }
    public function errorInfo()
    {
        return $this->pdo->errorInfo();
    }
    public function getAttribute(...$args)
    {
        return $this->pdo->getAttribute(...$args);
    }
}
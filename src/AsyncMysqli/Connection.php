<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncMysqli;

use Laminas\Db\Adapter\Driver\Mysqli\Mysqli;
use Revolt\EventLoop;
use Laminas\Db\Adapter\Exception;

class Connection extends \Laminas\Db\Adapter\Driver\Mysqli\Connection
{
    public const MAX_CONNECTION_RETRIES = 10;
    protected PooledLink $pooledLink;
    protected \WeakReference $driverRef;
    private static $mysqliReport;

    public function __construct($connectionInfo = null)
    {
        if ($connectionInfo instanceof PooledLink) {
            $this->pooledLink = $connectionInfo;
        } else {
            throw new Exception\InvalidArgumentException(
                sprintf('$connection must be instance of %s', PooledLink::class)
            );
        }
        self::$mysqliReport ??= \mysqli_report(\MYSQLI_REPORT_ERROR | \MYSQLI_REPORT_STRICT);
    }
    public function getCurrentSchema()
    {
        if (! $this->isConnected()) {
            $this->connect();
        }

        //$result = $this->getResource()->query('SELECT DATABASE()');
        $this->queryWithRetry('SELECT DATABASE()', \MYSQLI_ASYNC);

        $suspension = EventLoop::getSuspension();

        EventLoop::onMysqli($this->getResource(), static function (string $callbackId, \mysqli $link) use ($suspension) {
            \Revolt\EventLoop::cancel($callbackId);
            $suspension->resume($link->reap_async_query());
        });

        $result = $suspension->suspend();

        if ($result === false) {
            throw new Exception\RuntimeException(\mysqli_error($this->getResource()));
        }

        $result = $result->fetch_row();

        return $result[0];
    }

    public function execute($sql)
    {
        if (! $this->isConnected()) {
            $this->connect();
        }

        if ($this->profiler) {
            $this->profiler->profilerStart($sql);
        }

        //$result = $this->getResource()->query($sql);
        $this->queryWithRetry($sql, \MYSQLI_ASYNC);

        $suspension = EventLoop::getSuspension();

        EventLoop::onMysqli($this->getResource(), static function (string $callbackId, \mysqli $link) use ($suspension) {
            \Revolt\EventLoop::cancel($callbackId);
            $suspension->resume($link->reap_async_query());
        });

        $result = $suspension->suspend();

        if ($result === false) {
            throw new Exception\RuntimeException(\mysqli_error($this->getResource()));
        }

        if ($this->profiler) {
            $this->profiler->profilerFinish($sql);
        }

        return $this->getDriver()->createResult($result === true ? $this->getResource() : $result);
    }

    public function query(string $query, int $resultMode)
    {
        return $this->queryWithRetry($query, $resultMode);
    }

    private function queryWithRetry(string $query, int $resultMode)
    {
        $connectionErrors = [
            2006, // SQLSTATE[HY000]: General throwable: 2006 MySQL server has gone away
            2013,  // SQLSTATE[HY000]: General throwable: 2013 Lost connection to MySQL server during query
        ];
        $retryErrors = array_merge(
            $connectionErrors,
            [
                1213, // Deadlock found when trying to get lock; try restarting transaction
            ]
        );
        $triesCount = 0;
        do {
            $retry = false;
            try {
                return $this->getResource()->query($query, $resultMode);
            } catch (\mysqli_sql_exception $throwable) {
                if ($triesCount < Connection::MAX_CONNECTION_RETRIES
                    && in_array($throwable->getCode(), $retryErrors)
                ) {
                    $retry = true;
                    $triesCount++;
                    if (in_array($throwable->getCode(), $connectionErrors)) {
                        $this->getParentConnection()->reConnect();
                    }
                }

                if (!$retry) {
                    throw $throwable;
                }
            }
        } while ($retry);
    }

    public function getParentConnection(): ParentConnection
    {
        return $this->pooledLink->link;
    }

    public function getResource($pooled=false)
    {
        return $pooled ? $this->pooledLink : $this->pooledLink->link->getResource();
    }

    public function setResource(\mysqli $resource)
    {
        throw new Exception\RuntimeException(__METHOD__.' not supported');
    }

    public function beginTransaction()
    {
        if (! $this->isConnected()) {
            $this->connect();
        }

        $this->getResource()->autocommit(false);
        $this->inTransaction = true;

        return $this;
    }

    public function connect()
    {
        throw new Exception\RuntimeException(
            __METHOD__.' not supported. Connection should be provided from outside'
        );
    }

    public function commit()
    {
        if (! $this->isConnected()) {
            $this->connect();
        }

        $this->getResource()->commit();
        $this->inTransaction = false;
        $this->getResource()->autocommit(true);

        return $this;
    }

    public function rollback()
    {
        if (! $this->isConnected()) {
            throw new Exception\RuntimeException('Must be connected before you can rollback.');
        }

        if (! $this->inTransaction) {
            throw new Exception\RuntimeException('Must call beginTransaction() before you can rollback.');
        }

        $this->getResource()->rollback();
        $this->getResource()->autocommit(true);
        $this->inTransaction = false;

        return $this;
    }

    public function getLastGeneratedValue($name = null)
    {
        return $this->getResource()->insert_id;
    }

    public function isConnected()
    {
        return $this->getResource() instanceof \mysqli;
    }

    /**
     * {@inheritDoc}
     */
    public function disconnect()
    {
        throw new Exception\RuntimeException(__METHOD__.' not supported');
    }

    public function setDriver(Mysqli $driver)
    {
        $this->driverRef = \WeakReference::create($driver);

        return $this;
    }

    private function getDriver()
    {
        return $this->driverRef->get();
    }
}

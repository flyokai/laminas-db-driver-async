<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncMysqli;

use Laminas\Db\Adapter\Driver\Mysqli\Mysqli;
use Laminas\Db\Adapter\Exception;
use Laminas\Db\Adapter\ParameterContainer;
use Revolt\EventLoop;
use Laminas\Db\Adapter\Platform\Mysql as MysqlPlatform;

class Statement extends \Laminas\Db\Adapter\Driver\Mysqli\Statement
{
    public function prepare($sql = null)
    {
        if ($this->isPrepared) {
            throw new Exception\RuntimeException('This statement has already been prepared');
        }

        $this->isPrepared = true;
        return $this;
    }

    public function execute($parameters = null)
    {
        if (! $this->isPrepared) {
            $this->prepare();
        }

        /** START Standard ParameterContainer Merging Block */
        if (! $this->parameterContainer instanceof ParameterContainer) {
            if ($parameters instanceof ParameterContainer) {
                $this->parameterContainer = $parameters;
                $parameters               = null;
            } else {
                $this->parameterContainer = new ParameterContainer();
            }
        }

        if (is_array($parameters)) {
            $this->parameterContainer->setFromArray($parameters);
        }

        if ($this->parameterContainer->count() > 0) {
            $this->bindParametersFromContainer();
        } else {
            $this->bindedSql = $this->sql;
        }
        /** END Standard ParameterContainer Merging Block */

        if ($this->profiler) {
            $this->profiler->profilerStart($this);
        }

        $result = $this->executeWithRetry();

        if ($this->profiler) {
            $this->profiler->profilerFinish();
        }

        if ($result === false) {
            throw new Exception\RuntimeException(\mysqli_error($this->mysqli()));
        }

        if ($this->bufferResults === true) {
            $this->resource->store_result();
            $this->isPrepared = false;
            $buffered         = true;
        } else {
            $buffered = false;
        }

        return $this->getDriver()->createResult($result===true ? $this->mysqli() : $result, $buffered);
    }

    private function executeWithRetry(): mixed
    {
        $result = false;
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
                //$result = $this->mysqli()->query($this->bindedSql);
                //var_dump($this->bindedSql);
                $this->getConnection()->query($this->bindedSql, \MYSQLI_ASYNC);

                $suspension = EventLoop::getSuspension();

                EventLoop::onMysqli(
                    $this->mysqli(),
                    static function (string $callbackId, \mysqli $link) use ($suspension) {
                        \Revolt\EventLoop::cancel($callbackId);
                        try {
                            $suspension->resume($link->reap_async_query());
                        } catch (\Throwable $throwable) {
                            $suspension->throw($throwable);
                        }
                    }
                );

                $result = $suspension->suspend();

            } catch (\mysqli_sql_exception $throwable) {
                if ($triesCount < Connection::MAX_CONNECTION_RETRIES
                    && in_array($throwable->getCode(), $retryErrors)
                ) {
                    $retry = true;
                    $triesCount++;
                    if (in_array($throwable->getCode(), $connectionErrors)) {
                        $this->getConnection()->getParentConnection()->reConnect();
                    }
                }

                if (!$retry) {
                    throw new Exception\RuntimeException($throwable->getMessage(), previous: $throwable);
                }
            }
        } while ($retry);

        return $result;
    }

    /**
     * @var \WeakReference<Connection>
     */
    protected \WeakReference $connection;
    public function asyncInitialize(Connection $connection): self
    {
        $this->connection = \WeakReference::create($connection);
        return $this;
    }

    protected function getConnection(): Connection
    {
        return $this->connection->get();
    }

    protected function mysqli(): \mysqli
    {
        return $this->connection->get()->getResource();
    }

    public function initialize(\mysqli $mysqli)
    {
        throw new Exception\RuntimeException(__METHOD__.' not supported');
    }

    public function setResource(\mysqli_stmt $mysqliStatement)
    {
        throw new Exception\RuntimeException(__METHOD__.' not supported');
    }

    protected MysqlPlatform $platform;
    public function getPlatform(): MysqlPlatform
    {
        if (!isset($this->platform)) {
            $this->platform = new MysqlPlatform($this->getDriver());
        }
        return $this->platform;
    }

    protected $bindedSql = '';
    protected function bindParametersFromContainer()
    {
        $parameters = $this->parameterContainer->getNamedArray();
        $type       = '';
        $named      = [];
        $positioned = [];

        foreach ($parameters as $name => $value) {
            if ($this->parameterContainer->offsetHasErrata($name)) {
                switch ($this->parameterContainer->offsetGetErrata($name)) {
                    case ParameterContainer::TYPE_DOUBLE:
                        $value = floatval($value);
                        break;
                    case ParameterContainer::TYPE_NULL:
                        $value = 'NULL'; // as per @see http://www.php.net/manual/en/mysqli-stmt.bind-param.php#96148
                    case ParameterContainer::TYPE_INTEGER:
                        $value = intval($value);
                        break;
                    case ParameterContainer::TYPE_STRING:
                    default:
                        $value = $this->getPlatform()->quoteValue(strval($value));
                        break;
                }
            } else {
                if ($value === null) {
                    $value = 'NULL';
                } elseif (is_bool($value)) {
                    $value = intval($value);
                } elseif (!is_int($value) && !is_float($value)) {
                    $value = $this->getPlatform()->quoteValue(strval($value));
                }
            }
            if (is_int($name)) {
                $positioned[] = $value;
            } else {
                $name = str_starts_with($name, ':') ? $name : ':'.$name;
                $named[$name] = $value;
            }
        }

        uksort($named, fn($a, $b) => strlen($b) <=> strlen($a));

        $sqlArr = explode('?', $this->sql);
        $bindedSqlArr = [];
        foreach ($sqlArr as $__sql) {
            $bindedSqlArr[] = str_replace(array_keys($named), array_values($named), $__sql);
            $bindedSqlArr[] = (string)array_shift($positioned);
        }

        $this->bindedSql = implode('', $bindedSqlArr);
    }

    public function setDriver(Mysqli $driver)
    {
        $this->driver = \WeakReference::create($driver);
        return $this;
    }

    public function getDriver(): Mysqli
    {
        return $this->driver->get();
    }

}

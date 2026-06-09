<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo;

use Laminas\Db\Adapter\Driver\Mysqli\Mysqli;
use Laminas\Db\Adapter\Driver\Pdo\Pdo as LaminasPdoDriver;
use Laminas\Db\Adapter\Exception;
use Laminas\Db\Adapter\ParameterContainer;

/**
 * @property Resource\Pdo $pdo
 * @property Resource\Statement $resource
 */
class Statement extends \Laminas\Db\Adapter\Driver\Pdo\Statement
{
    public function initialize(\PDO $connectionResource)
    {
        throw new Exception\RuntimeException(__METHOD__.' method call is forbidden');
    }

    public function myInitialize(Resource\PDO $connectionResource)
    {
        $this->pdo = $connectionResource;
        return $this;
    }

    public function setResource(\PDOStatement $pdoStatement)
    {
        throw new Exception\RuntimeException(__METHOD__.' method call is forbidden');
    }

    public function setMyResource(Resource\Statement $resource)
    {
        $this->resource = $resource;
        return $this;
    }

    protected function bindParametersFromContainer()
    {
        if ($this->parametersBound) {
            return;
        }

        $parameters = $this->parameterContainer->getNamedArray();
        $__params = [];
        foreach ($parameters as $name => $value) {
            if (is_bool($value)) {
                $type = \PDO::PARAM_BOOL;
            } elseif (is_int($value)) {
                $type = \PDO::PARAM_INT;
            } else {
                $type = \PDO::PARAM_STR;
            }
            if ($this->parameterContainer->offsetHasErrata($name)) {
                switch ($this->parameterContainer->offsetGetErrata($name)) {
                    case ParameterContainer::TYPE_INTEGER:
                        $type = \PDO::PARAM_INT;
                        break;
                    case ParameterContainer::TYPE_NULL:
                        $type = \PDO::PARAM_NULL;
                        break;
                    case ParameterContainer::TYPE_LOB:
                        $type = \PDO::PARAM_LOB;
                        break;
                }
            }

            // parameter is named or positional, value is reference
            $parameter = is_int($name) ? $name + 1 : $this->getDriver()->formatParameterName($name);
            $__params[] = [$parameter, $value, $type];
        }
        $this->resource->bindParams($__params);
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
        }
        /** END Standard ParameterContainer Merging Block */

        if ($this->profiler) {
            $this->profiler->profilerStart($this);
        }

        try {
            $this->resource->execute();
        } catch (PDOException $e) {
            if ($this->profiler) {
                $this->profiler->profilerFinish();
            }

            $code = $e->getCode();
            if (! is_int($code)) {
                $code = 0;
            }

            throw new Exception\InvalidQueryException(
                'Statement could not be executed (' . implode(' - ', $this->resource->errorInfo()) . ')',
                $code,
                $e
            );
        }

        if ($this->profiler) {
            $this->profiler->profilerFinish();
        }

        return $this->getDriver()->createResult($this->resource, $this);
    }

    public function setDriver(LaminasPdoDriver $driver)
    {
        $this->driver = \WeakReference::create($driver);
        return $this;
    }

    public function getDriver(): LaminasPdoDriver
    {
        return $this->driver->get();
    }
}

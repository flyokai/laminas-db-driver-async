<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncMysqli;

use Laminas\Db\Adapter\Driver\Mysqli\Connection;
use Laminas\Db\Adapter\Driver\Mysqli\Result;
use Laminas\Db\Adapter\Exception;
use Laminas\Db\Adapter\Profiler;

class Driver extends \Laminas\Db\Adapter\Driver\Mysqli\Mysqli
{
    public function __construct(
        $connection,
        ?Statement $statementPrototype = null,
        ?Result $resultPrototype = null,
        array $options = []
    ) {
        if (! $connection instanceof Connection) {
            $connection = new Connection($connection);
        }

        $options = array_intersect_key(array_merge($this->options, $options), $this->options);

        $this->registerConnection($connection);
        $this->registerStatementPrototype($statementPrototype ?: new Statement($options['buffer_results']));
        $this->registerResultPrototype($resultPrototype ?: new Result());
    }

    public function formatParameterName($name, $type = null)
    {
        if ($type === null && ! is_numeric($name) || $type === self::PARAMETERIZATION_NAMED) {
            $name = ltrim($name, ':');
            // @see https://bugs.php.net/bug.php?id=43130
            if (preg_match('/[^a-zA-Z0-9_]/', $name)) {
                throw new Exception\RuntimeException(sprintf(
                    'The AsyncMysqli param %s contains invalid characters.'
                    . ' Only alphabetic characters, digits, and underscores (_)'
                    . ' are allowed.',
                    $name
                ));
            }
            return ':' . $name;
        }

        return '?';
    }

    public function createStatement($sqlOrResource = null)
    {
        /**
         * @todo Resource tracking
        if (is_resource($sqlOrResource) && !in_array($sqlOrResource, $this->resources, true)) {
        $this->resources[] = $sqlOrResource;
        }
         */

        $statement = clone $this->statementPrototype;
        if ($sqlOrResource instanceof mysqli_stmt) {
            throw new Exception\RuntimeException('mysqli_stmt is not supported in '.__METHOD__);
        } else {
            if (is_string($sqlOrResource)) {
                $statement->setSql($sqlOrResource);
            }
            if (! $this->connection->isConnected()) {
                $this->connection->connect();
            }
            $statement->asyncInitialize($this->connection);
        }
        return $statement;
    }

    /*protected \WeakReference $statementPrototypeRef;
    public function registerStatementPrototype(Statement $statementPrototype)
    {
        $statementPrototype->setDriver($this);
        $this->statementPrototypeRef = \WeakReference::create($statementPrototype);
    }

    public function getStatementPrototype()
    {
        return $this->statementPrototypeRef->get();
    }

    public function setProfiler(Profiler\ProfilerInterface $profiler)
    {
        $this->profiler = $profiler;
        if ($this->connection instanceof Profiler\ProfilerAwareInterface) {
            $this->connection->setProfiler($profiler);
        }
        if ($this->getStatementPrototype() instanceof Profiler\ProfilerAwareInterface) {
            $this->getStatementPrototype()->setProfiler($profiler);
        }
        return $this;
    }*/
}

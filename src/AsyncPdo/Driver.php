<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo;

/**
 * @property Connection $connection
 * @property Statement $statementPrototype
 * @property Result $resultPrototype
 */
class Driver extends \Laminas\Db\Adapter\Driver\Pdo\Pdo
{
    public function __construct(
        $connection,
        ?Statement $statementPrototype = null,
        ?Result $resultPrototype = null,
        $features = self::FEATURES_DEFAULT
    ) {
        if (!$connection instanceof Connection) {
            $connection = new Connection($connection);
        }
        $statementPrototype = $statementPrototype ?: new Statement();
        $resultPrototype = $resultPrototype ?: new Result();
        parent::__construct($connection, $statementPrototype, $resultPrototype, $features);
    }

    /**
     * @param Resource\Statement $resource
     * @param $context
     * @return Result
     */
    public function createResult($resource, $context = null)
    {
        $result   = clone $this->resultPrototype;
        $rowCount = null;

        // special feature, sqlite PDO counter
        if (
            $this->connection->getDriverName() === 'sqlite'
            && ($sqliteRowCounter = $this->getFeature('SqliteRowCounter'))
            && $resource->columnCount() > 0
        ) {
            $rowCount = $sqliteRowCounter->getRowCountClosure($context);
        }

        // special feature, oracle PDO counter
        if (
            $this->connection->getDriverName() === 'oci'
            && ($oracleRowCounter = $this->getFeature('OracleRowCounter'))
            && $resource->columnCount() > 0
        ) {
            $rowCount = $oracleRowCounter->getRowCountClosure($context);
        }

        $result->myInitialize($resource, $resource->pdo->lastInsertId(), $rowCount);
        return $result;
    }

    public function createStatement($sqlOrResource = null)
    {
        $statement = clone $this->statementPrototype;
        if ($sqlOrResource instanceof Resource\Statement) {
            $statement->setResource($sqlOrResource);
        } else {
            if (is_string($sqlOrResource)) {
                $statement->setSql($sqlOrResource);
            }
            if (! $this->connection->isConnected()) {
                $this->connection->connect();
            }
            $statement->myInitialize($this->connection->getResource());
        }
        return $statement;
    }
}
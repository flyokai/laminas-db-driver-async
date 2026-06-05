<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo;

use Amp\ByteStream\ClosedException;
use Amp\ByteStream\StreamException;
use Amp\Cache\CacheException;
use Amp\Cache\LocalCache;
use Amp\Cancellation;
use Amp\Parallel\Worker\Task;
use Amp\Sync\Channel;
use PDOStatement;

class PdoTask implements Task
{
    private static ?LocalCache $cache = null;
    private ?string $pdoId = null;

    /**
     * @param int|null $id File ID.
     *
     * @throws \Error
     */
    public function __construct(
        private readonly string  $operation,
        private readonly array   $args = [],
        private readonly ?string $id = null,
    )
    {
        if ($operation === '') {
            throw new \Error('Operation must be a non-empty string');
        }
    }

    protected function cache()
    {
        self::$cache ??= new LocalCache();
        return self::$cache;
    }

    protected function pdoPool(): Worker\PdoPool
    {
        if (!($pdoPool = $this->cache()->get('pdoPool'))) {
            $this->cache()->set('pdoPool', new Worker\PdoPool);
            $pdoPool = $this->cache()->get('pdoPool');
        }
        return $pdoPool;
    }

    /**
     * @throws \Error
     * @throws CacheException
     * @throws ClosedException
     * @throws StreamException
     */
    public function run(Channel $channel, Cancellation $cancellation): mixed
    {
        if ('connect' === $this->operation) {
            $pdo = $this->connect($this->args);
            [$pdoCacheId, $driverName] = $this->registerPdo($pdo);
            return [$pdoCacheId, $driverName];
        }

        $this->assertOperationName($this->operation);

        [$type, $method] = explode(':', $this->operation, 2);

        switch ($type) {
            case "statement":
                $this->assertStatementId($this->id);
                [,$this->pdoId] = explode(':', $this->id, 3);
                return $this->executeStatementMethod($method, $channel, $cancellation);
            case "pdo":
                [$this->pdoId] = explode(':', (string)$this->id, 2);
                return $this->executePdoMethod($method, $channel, $cancellation);

            default:
                throw new \Error("Invalid operation - " . $this->operation);
        }
    }

    public function assertStatementId($stmtId): void
    {
        if (count(explode(':', $stmtId))<3) {
            throw new \Error(sprintf("Invalid statement id '%s' [%s]", $stmtId, $this->operation));
        }
    }
    public function assertOperationName($operation): void
    {
        if (false === strpos($operation, ':')) {
            throw new \Error(sprintf("Invalid operation name '%s'" . $this->operation));
        }
    }

    protected function connect($params): Worker\PooledPdo
    {
        $this->assertConnectionParams($params);
        $pdo = $this->pdoPool()->getPdo($params);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        return $pdo;
    }

    protected function assertConnectionParams(array $params, bool $throw = true): bool
    {
        if (count($params)<1 || count($params)>4) {
            if ($throw) {
                throw new \Error(sprintf(
                    "Invalid number of connection params: %s [%s]", count($params), $this->operation
                ));
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * @return \PDO|null
     * @throws CacheException
     */
    private function pdo()
    {
        $this->assertPdoId();
        $pdo = $this->cache()->get($this->pdoCacheId($this->pdoId));
        if (!$pdo) {
            throw new \Error(
                sprintf("PDO resource not found '%s' [%s:%s]", $this->pdoId, $this->operation, $this->id)
            );
        }
        return $pdo;
    }

    private function assertPdoId()
    {
        if (!$this->pdoId) {
            throw new \Error(
                sprintf("PdoId is undefined [%s:%s]", $this->operation, $this->id)
            );
        }
    }

    private function pdoCacheId(Worker\PooledPdo|int $pdo): string
    {
        if ($pdo instanceof Worker\PooledPdo) {
            $pdo = spl_object_id($pdo);
        }
        return $pdo.':pdo';
    }

    private function driverNameCacheId(Worker\PooledPdo|int $pdo): string
    {
        if ($pdo instanceof Worker\PooledPdo) {
            $pdo = spl_object_id($pdo);
        }
        return $pdo.':driverName';
    }

    private function stmtCacheId(\PDOStatement|int $stmt): string
    {
        if ($stmt instanceof \PDOStatement) {
            $stmt = spl_object_id($stmt);
        }
        return $stmt.':'.$this->pdoId().':stmt';
    }

    /**
     * @return int|null
     * @throws CacheException
     */
    private function pdoId()
    {
        return ($pdo = $this->pdo())
            ? spl_object_id($pdo)
            : null;
    }

    private function executeStatementMethod(string $method, Channel $channel, Cancellation $cancellation)
    {
        $cache = $this->cache();
        $stmtCacheId = (string)$this->id;
        [$stmtId, $pdoId] = explode(':', $stmtCacheId, 3);
        $this->pdoId = $pdoId;
        $args = $this->args;
        /** @var PDOStatement $resource */
        $resource = $cache->get($stmtCacheId);
        if (!$resource) {
            throw new \Error("Statement resource not found - " . $stmtId);
        }
        assert($resource instanceof PDOStatement);
        switch ($method) {
            case 'fetch':
                return $resource->fetch(...$args);
            case 'bindParams':
                $result = true;
                foreach ($args as $param) {
                    $result = $result && $resource->bindParam(...$param);
                }
                return $result;
            case 'bindParam':
                return $resource->bindParam(...$args);
            case 'bindColumn':
                return $resource->bindColumn(...$args);
            case 'bindValue':
                return $resource->bindValue(...$args);
            case 'rowCount':
                return $resource->rowCount();
            case 'fetchColumn':
                return $resource->fetchColumn(...$args);
            case 'fetchAll':
                return $resource->fetchAll(...$args);
            case 'fetchObject':
                return $resource->fetchObject(...$args);
            case 'errorCode':
                return $resource->errorCode();
            case 'errorInfo':
                return $resource->errorInfo();
            case 'setAttribute':
                return $resource->setAttribute(...$args);
            case 'getAttribute':
                return $resource->getAttribute(...$args);
            case 'columnCount':
                return $resource->columnCount();
            case 'getColumnMeta':
                return $resource->getColumnMeta(...$args);
            case 'setFetchMode':
                return $resource->setFetchMode(...$args);
            case 'nextRowset':
                return $resource->nextRowset();
            case 'closeCursor':
                return $resource->closeCursor();
            case 'execute':
                return $resource->execute(...$args);
            case 'destroy':
                return $cache->delete($stmtCacheId);
            default:
                throw new \Error("Invalid operation - " . $this->operation);
        }
    }

    private function executePdoMethod(string $method, Channel $channel, Cancellation $cancellation)
    {
        $cache = $this->cache();
        $pdoId = $this->pdoId();
        /** @var Worker\PooledPdo $resource */
        $resource = $this->pdo();
        $args = $this->args;
        if (!$resource) {
            /*var_dump(sprintf("pdoId - %s - %s", $this->id, $this->pdoId));
            foreach ($this->cache() as $key => $value) {
                var_dump($key);
                var_dump($value);
            };*/
            throw new \Error(
                sprintf("PDO resource not found %s [%s:%s]", $pdoId, $this->operation, $this->id)
            );
        }
        if ($resource) {
            assert($resource instanceof Worker\PooledPdo);
        }
        switch ($method) {
            case 'prepare':
                $stmt = $resource->prepare(...$args);
                return $this->registerStatement($stmt);
            case 'beginTransaction':
                return $resource->beginTransaction();
            case 'commit':
                return $resource->commit();
            case 'rollBack':
                return $resource->rollBack();
            case 'inTransaction':
                return $resource->inTransaction();
            case 'setAttribute':
                return $resource->setAttribute(...$args);
            case 'exec':
                return $resource->exec(...$args);
            case 'query':
                $stmt = $resource->query(...$args);
                return $this->registerStatement($stmt);
            case 'lastInsertId':
                return $resource->lastInsertId(...$args);
            case 'errorCode':
                return $resource->errorCode();
            case 'errorInfo':
                return $resource->errorInfo();
            case 'getAttribute':
                return $resource->getAttribute(...$args);
            /*case 'quote':
                return $resource->quote(...$args);*/
            case 'destroy':
                $cache->delete($this->driverNameCacheId($pdoId));
                return $cache->delete($this->pdoCacheId($pdoId));
            case 'driverName':
                return $cache->get($this->driverNameCacheId($pdoId));
            default:
                throw new \Error("Invalid operation - " . $this->operation);
        }
    }

    private function registerPdo(Worker\PooledPdo $pdo)
    {
        $cache = $this->cache();
        $driverName = strtolower($pdo->getAttribute(\PDO::ATTR_DRIVER_NAME));
        $pdoCacheId = $this->pdoCacheId($pdo);
        $cache->set($pdoCacheId, $pdo);
        $cache->set($this->driverNameCacheId($pdo), $driverName);
        return [$pdoCacheId, $driverName];
    }

    private function registerStatement(\PDOStatement $stmt)
    {
        $cache = $this->cache();
        $stmtCacheId = $this->stmtCacheId($stmt);
        $cache->set($stmtCacheId, $stmt);
        return $stmtCacheId;
    }
}
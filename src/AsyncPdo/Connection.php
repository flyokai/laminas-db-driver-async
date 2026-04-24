<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo;

use Amp\ByteStream\StreamException;
use Amp\Parallel\Worker\TaskFailureException;
use Amp\Parallel\Worker\WorkerException;
use Laminas\Db\Adapter\Driver\Pdo\Pdo as LaminasPdoDriver;
use Revolt\EventLoop\FiberLocal;
use Laminas\Db\Adapter\Exception;

class Connection extends \Laminas\Db\Adapter\Driver\Pdo\Connection
{
    private ?PdoWorkerPool $pool = null;
    /**
     * @var FiberLocal|int
     */
    private mixed $transactionLevel;
    /**
     * @var FiberLocal|bool
     */
    private mixed $isRolledBack;
    /**
     * @var FiberLocal|Resource\WorkerPdo
     */
    private mixed $pdoResource;
    /**
     * @var FiberLocal|string
     */
    private mixed $pdoDriverName;

    private bool $fiberMode = true;

    public function __construct($connectionParameters = null)
    {
        if (is_array($connectionParameters)) {
            $this->setConnectionParameters($connectionParameters);
        } elseif (null !== $connectionParameters) {
            throw new Exception\InvalidArgumentException(
                '$connection must be an array of parameters, a PDO object or null'
            );
        }
        if ($this->fiberMode) {
            $this->transactionLevel = new FiberLocal(static fn () => 0);
            $this->isRolledBack = new FiberLocal(static fn () => false);
            $this->pdoResource = new FiberLocal(static fn () => null);
            $this->pdoDriverName = new FiberLocal(static fn () => null);
        } else {
            $this->transactionLevel = 0;
            $this->isRolledBack = false;
            $this->pdoResource = null;
            $this->pdoDriverName = null;
        }
    }

    public function setConnectionParameters(array $connectionParameters)
    {
        if (isset($connectionParameters['worker_pool'])) {
            $this->setPool($connectionParameters['worker_pool']);
            unset($connectionParameters['worker_pool']);
        }
        if (array_key_exists('fiber_mode', $connectionParameters) && !$connectionParameters['fiber_mode']) {
            $this->fiberMode = false;
        } else {
            $this->fiberMode = true;
        }
        unset($connectionParameters['fiber_mode']);
        parent::setConnectionParameters($connectionParameters);
    }

    public function getCurrentSchema()
    {
        if (! $this->isConnected()) {
            $this->connect();
        }

        switch ($this->driverName) {
            case 'mysql':
                $sql = 'SELECT DATABASE()';
                break;
            case 'sqlite':
                return 'main';
            case 'sqlsrv':
            case 'dblib':
                $sql = 'SELECT SCHEMA_NAME()';
                break;
            case 'pgsql':
            default:
                $sql = 'SELECT CURRENT_SCHEMA';
                break;
        }

        /** @var Resource\Statement $result */
        $result = $this->getResource()->query($sql);
        if ($result instanceof Resource\Statement) {
            return $result->fetchColumn();
        }

        return false;
    }

    public function connect()
    {
        if ($this->isConnected()) {
            return $this;
        }

        if (!$this->pool) {
            throw new Exception\RuntimeException("Worker pool is not specified");
        }

        $dsn     = $username = $password = $hostname = $database = null;
        $options = [];
        foreach ($this->connectionParameters as $key => $value) {
            switch (strtolower($key)) {
                case 'dsn':
                    $dsn = $value;
                    break;
                case 'driver':
                    $value = strtolower((string) $value);
                    if (strpos($value, 'pdo') === 0) {
                        $pdoDriver = str_replace(['-', '_', ' '], '', $value);
                        $pdoDriver = substr($pdoDriver, 3) ?: '';
                    }
                    break;
                case 'pdodriver':
                    $pdoDriver = (string) $value;
                    break;
                case 'user':
                case 'username':
                    $username = (string) $value;
                    break;
                case 'pass':
                case 'password':
                    $password = (string) $value;
                    break;
                case 'host':
                case 'hostname':
                    $hostname = (string) $value;
                    break;
                case 'port':
                    $port = (int) $value;
                    break;
                case 'database':
                case 'dbname':
                    $database = (string) $value;
                    break;
                case 'charset':
                    $charset = (string) $value;
                    break;
                case 'unix_socket':
                    $unixSocket = (string) $value;
                    break;
                case 'version':
                    $version = (string) $value;
                    break;
                case 'driver_options':
                case 'options':
                    $value   = (array) $value;
                    $options = array_diff_key($options, $value) + $value;
                    break;
                default:
                    $options[$key] = $value;
                    break;
            }
        }

        if (isset($hostname) && isset($unixSocket)) {
            throw new Exception\InvalidConnectionParametersException(
                'Ambiguous connection parameters, both hostname and unix_socket parameters were set',
                $this->connectionParameters
            );
        }

        if (! isset($dsn) && isset($pdoDriver)) {
            $dsn = [];
            switch ($pdoDriver) {
                case 'sqlite':
                    $dsn[] = $database;
                    break;
                case 'sqlsrv':
                    if (isset($database)) {
                        $dsn[] = "database={$database}";
                    }
                    if (isset($hostname)) {
                        $dsn[] = "server={$hostname}";
                    }
                    break;
                default:
                    if (isset($database)) {
                        $dsn[] = "dbname={$database}";
                    }
                    if (isset($hostname)) {
                        $dsn[] = "host={$hostname}";
                    }
                    if (isset($port)) {
                        $dsn[] = "port={$port}";
                    }
                    if (isset($charset) && $pdoDriver !== 'pgsql') {
                        $dsn[] = "charset={$charset}";
                    }
                    if (isset($unixSocket)) {
                        $dsn[] = "unix_socket={$unixSocket}";
                    }
                    if (isset($version)) {
                        $dsn[] = "version={$version}";
                    }
                    break;
            }
            $dsn = $pdoDriver . ':' . implode(';', $dsn);
        } elseif (! isset($dsn)) {
            throw new Exception\InvalidConnectionParametersException(
                'A dsn was not provided or could not be constructed from your parameters',
                $this->connectionParameters
            );
        }

        $this->dsn = $dsn;

        try {
            $worker = $this->pool->createPooledWorker();
            [$pdoId, $driverName] = $worker->execute(
                new PdoTask('connect', [$dsn, $username, $password, $options])
            );
            $pdoResource = new Resource\WorkerPdo($pdoId, $worker);
            if ($this->fiberMode) {
                $this->pdoResource->set($pdoResource);
                $this->pdoDriverName->set($driverName);
            } else {
                $this->pdoResource = $pdoResource;
                $this->pdoDriverName = $driverName;
            }
        } catch (TaskFailureException $exception) {
            throw new Exception\RuntimeException("Reading from the worker failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new Exception\RuntimeException("Sending the task to the worker failed", 0, $exception);
        }

        return $this;
    }

    public function disconnect()
    {
        if ($this->isConnected()) {
            if ($this->fiberMode) {
                $this->pdoResource->set(null);
            } else {
                $this->pdoResource = null;
            }
        }

        return $this;
    }

    public function isConnected()
    {
        return $this->fiberMode ? $this->pdoResource->get() : $this->pdoResource;
    }

    public function beginTransaction()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        if ($this->isRolledBack()) {
            throw new Exception\RuntimeException('Rolled back transaction has not been completed correctly.');
        }

        if (0 === $this->transactionLevel()) {
            $this->getResource()->beginTransaction();
            $this->inTransaction = true;
        }

        $this->incTransactionLevel();

        return $this;
    }

    private function decTransactionLevel(int $dec=1)
    {
        $this->incTransactionLevel(-1*$dec);
    }
    private function incTransactionLevel(int $inc=1)
    {
        $this->transactionLevel(true, $this->transactionLevel() + $inc);
    }

    private function transactionLevel(bool $setFlag=false, int $value=0)
    {
        $transactionLevel = $this->fiberMode ? $this->transactionLevel->get() : $this->transactionLevel;
        if ($setFlag) {
            if ($this->fiberMode) {
                $this->transactionLevel->set($value);
            } else {
                $this->transactionLevel = $value;
            }
        }
        return $transactionLevel;
    }

    private function isRolledBack(bool $setFlag=false, bool $value=false)
    {
        $isRolledBack = $this->fiberMode ? $this->isRolledBack->get() : $this->isRolledBack;
        if ($setFlag) {
            if ($this->fiberMode) {
                $this->isRolledBack->set($value);
            } else {
                $this->isRolledBack = $value;
            }
        }
        return $isRolledBack;
    }

    public function commit()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }
        if ($this->transactionLevel() === 1 && !$this->isRolledBack()) {
            $this->_commit();
        } elseif ($this->transactionLevel() === 0) {
            throw new Exception\RuntimeException('Asymmetric transaction commit.');
        } elseif ($this->isRolledBack()) {
            throw new Exception\RuntimeException('Rolled back transaction has not been completed correctly.');
        }
        $this->decTransactionLevel();
        return $this;
    }

    protected function _commit()
    {
        $this->getResource()->commit();
    }

    public function rollBack()
    {
        if ($this->transactionLevel() === 1) {
            $this->_rollBack();
            $this->isRolledBack(true, false);
        } elseif ($this->transactionLevel() === 0) {
            throw new Exception\RuntimeException('Asymmetric transaction rollback.');
        } else {
            $this->isRolledBack(true, true);
        }
        $this->decTransactionLevel();
        return $this;
    }

    protected function _rollback()
    {
        $this->getResource()->rollback();
    }

    public function execute($sql)
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        if ($this->profiler) {
            $this->profiler->profilerStart($sql);
        }

        $resultResource = $this->getResource()->query($sql);

        if ($this->profiler) {
            $this->profiler->profilerFinish($sql);
        }

        return $this->getDriver()->createResult($resultResource, $sql);
    }

    public function getLastGeneratedValue($name = null)
    {
        if (
            $name === null
            && ($this->driverName === 'pgsql' || $this->driverName === 'firebird')
        ) {
            return;
        }

        try {
            return $this->getResource()->lastInsertId($name);
        } catch (\Exception $e) {
            // do nothing
        }

        return false;
    }

    public function getDriverName()
    {
        return $this->driverName;
    }

    public function setResource(\PDO $resource)
    {
        throw new Exception\RuntimeException(__METHOD__.' method call is forbidden');
    }

    /**
     * @return Resource\WorkerPdo|null
     */
    public function getResource()
    {
        return $this->fiberMode ? $this->pdoResource->get() : $this->pdoResource;
    }

    public function setPool(PdoWorkerPool $pool)
    {
        $this->pool = $pool;
        return $this;
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

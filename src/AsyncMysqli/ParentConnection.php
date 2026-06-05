<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncMysqli;

use Exception as GenericException;
use Laminas\Db\Adapter\Driver\AbstractConnection;
use Laminas\Db\Adapter\Exception;
use Laminas\Db\Adapter\Exception\InvalidArgumentException;

class ParentConnection extends \Laminas\Db\Adapter\Driver\Mysqli\Connection
{
    private array $realConnectParams;

    public function reConnect()
    {
        if (!$this->resource instanceof \mysqli) {
            throw new \LogicException('mysqli::reConnect - connection resource must be a valid mysqli object');
        }
        $this->_realConnect();
    }
    public function connect()
    {
        if ($this->resource instanceof \mysqli) {
            return $this;
        }
        $this->_connect();
        $this->_realConnect();
    }

    private function _connect()
    {
        // localize
        $p = $this->connectionParameters;

        // given a list of key names, test for existence in $p
        $findParameterValue = function (array $names) use ($p) {
            foreach ($names as $name) {
                if (isset($p[$name])) {
                    return $p[$name];
                }
            }

            return null;
        };

        $this->realConnectParams['hostname'] = $findParameterValue(['hostname', 'host']);
        $this->realConnectParams['username'] = $findParameterValue(['username', 'user']);
        $this->realConnectParams['password'] = $findParameterValue(['password', 'passwd', 'pw']);
        $this->realConnectParams['database'] = $findParameterValue(['database', 'dbname', 'db', 'schema']);
        $this->realConnectParams['port']     = isset($p['port']) ? (int) $p['port'] : null;
        $this->realConnectParams['socket']   = $p['socket'] ?? null;
        $this->realConnectParams['initStatements']   = $p['initStatements'] ?? [];

        // phpcs:ignore WebimpressCodingStandard.NamingConventions.ValidVariableName.NotCamelCaps
        $useSSL     = $p['use_ssl'] ?? 0;
        $clientKey  = $p['client_key'] ?? '';
        $clientCert = $p['client_cert'] ?? '';
        $caCert     = $p['ca_cert'] ?? '';
        $caPath     = $p['ca_path'] ?? '';
        $cipher     = $p['cipher'] ?? '';

        $this->resource = $this->createResource();

        if (! empty($p['driver_options'])) {
            foreach ($p['driver_options'] as $option => $value) {
                if (is_string($option)) {
                    $option = strtoupper($option);
                    if (! defined($option)) {
                        continue;
                    }
                    $option = constant($option);
                }
                $this->resource->options($option, $value);
            }
        }

        $flags = null;

        // phpcs:ignore WebimpressCodingStandard.NamingConventions.ValidVariableName.NotCamelCaps
        if ($useSSL && ! $this->realConnectParams['socket']) {
            // Even though mysqli docs are not quite clear on this, MYSQLI_CLIENT_SSL
            // needs to be set to make sure SSL is used. ssl_set can also cause it to
            // be implicitly set, but only when any of the parameters is non-empty.
            $flags = MYSQLI_CLIENT_SSL;
            $this->resource->ssl_set($clientKey, $clientCert, $caCert, $caPath, $cipher);
            //MYSQLI_CLIENT_SSL_DONT_VERIFY_SERVER_CERT is not valid option, needs to be set as flag
            if (
                isset($p['driver_options'])
                && isset($p['driver_options'][MYSQLI_CLIENT_SSL_DONT_VERIFY_SERVER_CERT])
            ) {
                $flags |= MYSQLI_CLIENT_SSL_DONT_VERIFY_SERVER_CERT;
            }
        }

        $this->realConnectParams['flags'] = $flags;
    }

    /**
     * {@inheritDoc}
     */
    private function _realConnect()
    {
        extract($this->realConnectParams);
        try {
            $flags === null
                ? $this->resource->real_connect($hostname, $username, $password, $database, $port, $socket)
                : $this->resource->real_connect($hostname, $username, $password, $database, $port, $socket, $flags);
        } catch (GenericException $e) {
            throw new Exception\RuntimeException(
                'Connection error',
                $this->resource->connect_errno,
                new Exception\ErrorException($this->resource->connect_error, $this->resource->connect_errno)
            );
        }

        if ($this->resource->connect_error) {
            throw new Exception\RuntimeException(
                'Connection error',
                $this->resource->connect_errno,
                new Exception\ErrorException($this->resource->connect_error, $this->resource->connect_errno)
            );
        }

        if (! empty($p['charset'])) {
            $this->resource->set_charset($p['charset']);
        }

        foreach ($initStatements as $statement) {
            $this->resource->query($statement);
        }

        return $this;
    }
}

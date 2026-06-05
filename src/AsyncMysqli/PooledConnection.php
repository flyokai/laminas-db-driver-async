<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncMysqli;

use Laminas\Db\Adapter\Driver\Mysqli\Connection as MysqliConnection;

class PooledConnection
{
    /**
     * @param \Closure(MysqliConnection):void $push Closure to push the mysqli link back into the queue.
     */
    public function __construct(
        public readonly MysqliConnection $connection,
        private readonly \Closure   $push
    ) {
    }

    /**
     * Automatically pushes the mysqli link back into the queue.
     */
    public function __destruct()
    {
        ($this->push)($this->connection);
    }

    public function __call($name, $args): mixed
    {
        return call_user_func_array([$this->connection, $name], $args);
    }

}

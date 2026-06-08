<?php

namespace Flyokai\LaminasDbDriverAsync;

use Laminas\Db\Adapter\Adapter;
use Laminas\Db\Sql\Sql;

trait CreateSqlTrait
{
    private function createSql(): Sql
    {
        return new Sql($this->createAdapter());
    }

    private function createAdapter(): Adapter
    {
        return $this->connectionPool->createConnection();
    }
}

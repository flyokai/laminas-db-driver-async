<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo;

use Laminas\Db\Adapter\Exception;

/**
 * @property Resource\Statement $resource
 */
class Result extends \Laminas\Db\Adapter\Driver\Pdo\Result
{
    protected $isBuffered = false;
    protected $buffer = null;
    protected int $idx = 0;

    public function initialize(\PDOStatement $resource, $generatedValue, $rowCount = null)
    {
        throw new Exception\RuntimeException(__METHOD__.' method call is forbidden');
    }

    public function myInitialize(Resource\Statement $resource, $generatedValue, $rowCount = null)
    {
        $this->resource       = $resource;
        $this->generatedValue = $generatedValue;
        $this->rowCount       = $rowCount;
        $this->isBuffered     = false;
        $this->buffer         = null;
        return $this;
    }

    public function buffer()
    {
        if ($this->buffer === null) {
            $this->buffer = $this->resource->fetchAll($this->fetchMode);
            $this->isBuffered = true;
        }
    }

    /**
     * @return bool|null
     */
    public function isBuffered()
    {
        return $this->isBuffered;
    }

    public function current()
    {
        $this->buffer();
        return \current($this->buffer);
    }

    public function key()
    {
        $this->buffer();
        return \key($this->buffer);
    }

    public function next()
    {
        $this->buffer();
        $this->idx++;
        return \next($this->buffer);
    }

    public function valid()
    {
        $this->buffer();
        return $this->count() > $this->idx;
    }

    public function rewind(): void {
        $this->buffer();
        \reset($this->buffer);
        $this->idx = 0;
    }

}
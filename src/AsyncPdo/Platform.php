<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo;

class Platform extends \Laminas\Db\Adapter\Platform\Mysql
{
    public function quoteValue($value)
    {
        if (is_int($value)) {
            return $value;
        } elseif (is_float($value)) {
            return sprintf('%F', $value);
        }
        return '\'' . addcslashes((string) $value, "\x00\n\r\\'\"\x1a") . '\'';
    }

    public function setDriver($driver)
    {
        if ($driver instanceof Driver) {
            $this->driver = $driver;
            return $this;
        }
        return parent::setDriver($driver);
    }
}
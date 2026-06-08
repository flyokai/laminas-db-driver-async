<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo\Resource;

interface Statement
{
    public function execute($params = null): bool;
    public function fetch($mode = \PDO::FETCH_DEFAULT, $cursorOrientation = \PDO::FETCH_ORI_NEXT, $cursorOffset = 0): mixed;
    public function bindParams($params): bool;
    public function bindParam($param, $var, $type = \PDO::PARAM_STR, $maxLength = 0, $driverOptions = null): bool;
    public function bindColumn($column, $var, $type = \PDO::PARAM_STR, $maxLength = 0, $driverOptions = null): bool;
    public function bindValue($param, $value, $type = \PDO::PARAM_STR): bool;
    public function rowCount(): int;
    public function fetchColumn($column = 0): mixed;
    public function fetchAll($mode = \PDO::FETCH_DEFAULT, ...$args): array;
    public function fetchObject($class = "stdClass",$constructorArgs = []): object|false;
    public function errorCode(): ?string;
    public function errorInfo(): array;
    public function setAttribute($attribute,$value): bool;
    public function getAttribute($name): mixed;
    public function columnCount(): int;
    public function getColumnMeta($column): array|false;
    public function setFetchMode(...$params): bool;
    public function nextRowset(): bool;
    public function closeCursor(): bool;
}
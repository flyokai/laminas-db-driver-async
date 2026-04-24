<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo\Resource;

interface Pdo
{
    public function prepare(string $query, array $options = []): Statement|false;
    public function beginTransaction(): bool;
    public function commit(): bool;
    public function rollBack(): bool;
    public function inTransaction(): bool;
    public function setAttribute(int $attribute, mixed $value): bool;
    public function exec(string $statement): int|false;
    public function query(string $query, ?int $fetchMode = null, ...$fetch_mode_args): Statement|false;
    public function lastInsertId($name = null): string|false;
    public function errorCode(): ?string;
    public function errorInfo(): array;
    public function getAttribute(int $attribute): mixed;
    public function quote(string $string, int $type = \PDO::PARAM_STR): string|false;

}
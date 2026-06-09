<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncPdo;

class PendingOperationError extends \Error
{
    public function __construct(
        string $message = "The previous pdo operation must complete before another can be started",
        ?\Throwable $previous = null
    ) {
        parent::__construct($message, 0, $previous);
    }
}
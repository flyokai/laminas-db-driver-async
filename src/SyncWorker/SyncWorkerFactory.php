<?php

namespace Flyokai\LaminasDbDriverAsync\SyncWorker;

use Amp\Cancellation;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Parallel\Context\ContextFactory;
use Amp\Parallel\Worker\Worker;
use Amp\Parallel\Worker\WorkerFactory;
use function Amp\Parallel\Context\contextFactory;

class SyncWorkerFactory implements WorkerFactory
{
    use ForbidCloning;
    use ForbidSerialization;

    public const SCRIPT_PATH = __DIR__ . "/sync-task-runner.php";

    public function __construct(
        private readonly ?string $bootstrapPath = null,
        private readonly ?ContextFactory $contextFactory = null,
        private readonly array $arguments = []
    ) {
        if ($this->bootstrapPath !== null && !\file_exists($this->bootstrapPath)) {
            throw new \Error(\sprintf("No file found at bootstrap path given '%s'", $this->bootstrapPath));
        }
    }

    public function create(?Cancellation $cancellation = null): Worker
    {
        $script = [self::SCRIPT_PATH];

        if ($this->bootstrapPath !== null) {
            $script[] = $this->bootstrapPath;
        }
        array_push($script, ...$this->arguments);

        return new SyncContextWorker(($this->contextFactory ?? contextFactory())->start($script, $cancellation));
    }
}
<?php declare(strict_types=1);

namespace Amp\Parallel\Worker\Internal;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\Parallel\Worker\Internal;
use Amp\Parallel\Worker\Task;
use Amp\Pipeline\Queue;
use Amp\Serialization\SerializationException;
use Amp\Sync\Channel;
use Revolt\EventLoop;

return static function (Channel $channel) use ($argc, $argv): int {
    if (!\defined("AMP_WORKER")) {
        \define("AMP_WORKER", \AMP_CONTEXT);
    }

    if (isset($argv[1])) {
        if (!\is_file($argv[1])) {
            throw new \Error(\sprintf("No file found at bootstrap path given '%s'", $argv[1]));
        }

        // Include file within closure to protect scope.
        (function () use ($argc, $argv): void {
            /** @psalm-suppress UnresolvableInclude */
            require $argv[1];
        })();
    }

    /** @var array<string, DeferredCancellation> $cancellation */
    $cancellation = [];

    while ($task = $channel->receive()) {
        if ($task instanceof Task) {
            try {
                $cancellation = new DeferredCancellation;

                $result = $task->run($channel, $cancellation->getCancellation());

                if ($result instanceof Future) {
                    $result = $result->await($cancellation->getCancellation());
                }

                $result = new Internal\TaskSuccess('a', $result);
            } catch (\Throwable $exception) {
                if ($exception instanceof CancelledException && $cancellation->isCancelled()) {
                    $result = new Internal\TaskCancelled('a', $exception);
                } else {
                    $result = new Internal\TaskFailure('a', $exception);
                }
            } finally {
                try {
                    $channel->send($result);
                } catch (SerializationException $exception) {
                    // Could not serialize task result.
                    $channel->send(new Internal\TaskFailure('a', $exception));
                }
            }
            continue;
        }

        if ($task instanceof Internal\JobCancellation) {
            $cancellation?->cancel();
            continue;
        }

        // Should not happen, but just in case...
        throw new \Error('Invalid value ' . \get_debug_type($task) . ' received in ' . __FUNCTION__);
    }

    return 0;
};

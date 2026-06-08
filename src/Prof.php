<?php declare(strict_types=1);

namespace Flyokai\LaminasDbDriverAsync;

/**
 * Tiny env-gated profiler for the async DB path. No-op unless FUJIN_PROF=1.
 *
 * Lives in the driver package so both the driver (Statement) and downstream
 * consumers (fujin-shuttle deputy/depot) can record into one accumulator without
 * a cross-package service dependency. Records, per label: call count + total
 * microseconds. Also tracks a global in-flight gauge so we can see the real
 * async concurrency achieved (max simultaneous suspended queries).
 */
final class Prof
{
    public static bool $on = false;

    /** @var array<string, array{0:int,1:float}> label => [count, totalUs] */
    private static array $acc = [];

    public static int $inflight = 0;
    public static int $maxInflight = 0;

    public static function init(): void
    {
        self::$on = \getenv('FUJIN_PROF') === '1';
    }

    public static function add(string $label, float $us): void
    {
        if (!self::$on) {
            return;
        }
        $a = self::$acc[$label] ?? [0, 0.0];
        $a[0]++;
        $a[1] += $us;
        self::$acc[$label] = $a;
    }

    public static function enter(): void
    {
        if (!self::$on) {
            return;
        }
        self::$inflight++;
        if (self::$inflight > self::$maxInflight) {
            self::$maxInflight = self::$inflight;
        }
    }

    public static function leave(): void
    {
        if (!self::$on) {
            return;
        }
        self::$inflight--;
    }

    /**
     * @template T
     * @param \Closure():T $fn
     * @return T
     */
    public static function measure(string $label, \Closure $fn)
    {
        if (!self::$on) {
            return $fn();
        }
        $t = \microtime(true);
        try {
            return $fn();
        } finally {
            self::add($label, (\microtime(true) - $t) * 1e6);
        }
    }

    public static function reset(): void
    {
        self::$acc = [];
        self::$inflight = 0;
        self::$maxInflight = 0;
    }

    public static function report(): string
    {
        if (!self::$on) {
            return '';
        }
        $rows = self::$acc;
        \uasort($rows, static fn ($a, $b) => $b[1] <=> $a[1]);
        $out = \sprintf("\n=== Prof (max concurrent in-flight queries: %d) ===\n", self::$maxInflight);
        $out .= \sprintf("%-32s %10s %14s %12s\n", 'label', 'count', 'total_ms', 'avg_ms');
        foreach ($rows as $label => [$count, $us]) {
            $out .= \sprintf(
                "%-32s %10d %14.1f %12.3f\n",
                $label,
                $count,
                $us / 1000,
                $count > 0 ? ($us / 1000 / $count) : 0,
            );
        }
        return $out;
    }
}

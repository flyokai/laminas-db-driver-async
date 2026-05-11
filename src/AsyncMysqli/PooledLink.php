<?php

namespace Flyokai\LaminasDbDriverAsync\AsyncMysqli;

class PooledLink
{
    /**
     * @param \Closure(ParentConnection):void $push Closure to push the mysqli link back into the queue.
     * @param \Closure():void|null $onRelease Optional hook fired BEFORE the link is pushed back —
     *        used by LinkPool to remove the acquire entry from its tracking map. Kept separate
     *        from $push so the original release semantics stay unchanged when no hook is set.
     */
    public function __construct(
        public readonly ParentConnection  $link,
        private readonly \Closure $push,
        private readonly ?\Closure $onRelease = null,
    ) {
    }

    /**
     * Automatically pushes the mysqli link back into the queue.
     */
    public function __destruct()
    {
        if ($this->onRelease !== null) {
            ($this->onRelease)();
        }
        ($this->push)($this->link);
    }

    public function __call($name, $args): mixed
    {
        return call_user_func_array([$this->link, $name], $args);
    }

}

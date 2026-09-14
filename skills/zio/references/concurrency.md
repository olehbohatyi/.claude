# Concurrency & Parallelism

## Fibers

A fiber is a lightweight, interruptible virtual thread. Millions can exist; they multiplex over a small OS thread pool.

```scala
val fiber: ZIO[R, Nothing, Fiber[E, A]] = effect.fork
fiber.join        // ZIO[R, E, A]        — propagates failure
fiber.await       // ZIO[R, Nothing, Exit[E, A]] — never fails
fiber.interrupt   // ZIO[R, Nothing, Exit[E, A]] — waits for finalizers
fiber.poll        // ZIO[R, Nothing, Option[Exit[E, A]]]
```

**Prefer high-level operators over manual `fork`.** `zipPar`, `race`, `foreachPar`, `collectAllPar`, `mergeAllPar`, `validatePar` all handle supervision and interruption correctly.

### Structured concurrency

A fiber forked with `fork` is a **child** of the fiber that forked it. When the parent completes, its children are interrupted. This is the default and it is what you want — it prevents fiber leaks.

Escape hatches, use deliberately:

- `forkDaemon` — child of the *global* scope; survives the parent. You now own its lifecycle.
- `forkScoped` — lifetime bound to the surrounding `Scope`.
- `forkIn(scope)` — bound to a specific scope.

`fork` then `join` is identity-preserving: `effect.fork.flatMap(_.join) === effect` (modulo interruptibility).

### Fiber-local state

`FiberRef[A]` is a value each fiber sees independently. Children inherit the parent's value at fork time, and a `join` can merge the child's value back (configurable via `fork`/`join` functions in `FiberRef.make`). This is how `ZIO.logAnnotate`, log spans and regional config work.

```scala
val ref = FiberRef.make(Map.empty[String, String])
ZIO.scoped { ref.flatMap(r => r.locally(Map("k" -> "v"))(effect)) }
```

## Interruption

Interruption is **prompt** and **cooperative**: it takes effect at effect boundaries, and a fiber always runs its finalizers before dying.

```scala
ZIO.uninterruptible(criticalSection)
ZIO.interruptible(effect)
ZIO.uninterruptibleMask { restore =>
  acquire *> restore(use).onExit(_ => release)   // the acquireRelease pattern
}
effect.disconnect      // interruption returns immediately; finalizers run in background
effect.onInterrupt(cleanup)
ZIO.checkInterruptible(status => ...)
```

Rules:
- Acquisition of a resource must be uninterruptible; usage must be interruptible. `ZIO.acquireRelease` does this for you — use it rather than hand-rolling masks.
- Never make a long-running effect uninterruptible; you make the whole app unkillable.
- `effect.timeout(d)` interrupts on expiry and **waits** for finalizers. `timeoutFork` doesn't wait.

## Parallel operators

```scala
a zipPar b                                  // both, fail fast on either
a race b                                    // first to *succeed*; loser interrupted
a raceFirst b                               // first to *complete* (success or failure)
a.raceWith(b)(leftDone, rightDone)          // full control
ZIO.foreachPar(xs)(f)                       // unbounded parallelism
ZIO.foreachPar(xs)(f).withParallelism(8)    // bounded — almost always what you want
ZIO.collectAllSuccessesPar(xs)              // ignore failures
ZIO.mergeAllPar(xs)(zero)(combine)
ZIO.raceAll(xs)
```

`foreachPar` with unbounded parallelism over a large collection will happily open 10 000 connections. Bound it with `withParallelism` or a `Semaphore`.

## Shared state

### Ref

```scala
for {
  ref <- Ref.make(0)
  _   <- ref.update(_ + 1)
  _   <- ref.updateAndGet(_ * 2)
  out <- ref.modify(s => (s.toString, s + 1))   // compute a result AND a new state
  v   <- ref.get
} yield out
```

Each operation is atomic. **Two operations are not atomic together.** This is the single most common concurrency bug in ZIO code:

```scala
// BROKEN — another fiber can interleave between get and set
ref.get.flatMap(v => ref.set(v + 1))
// CORRECT
ref.update(_ + 1)
```

If the combined operation spans two `Ref`s, you need `STM`.

### Ref.Synchronized

When updating requires running an effect (e.g. refresh a token by calling an API). Updates are serialized; other fibers block during the effect — keep it short.

```scala
refSync.updateZIO(state => fetchNewToken.map(state.withToken))
```

### Promise

A write-once variable used to synchronize fibers.

```scala
for {
  p <- Promise.make[Nothing, Int]
  _ <- (ZIO.sleep(1.second) *> p.succeed(42)).fork
  v <- p.await            // suspends, does not block a thread
} yield v
```

Completion: `succeed`, `fail`, `die`, `complete(effect)` (runs once, all waiters get the same result), `completeWith(effect)` (each waiter runs it). `p.await` is interruptible.

`Ref` + `Promise` is the standard recipe for caches/de-duplication: store `Map[K, Promise[E, V]]` in a `Ref` so concurrent requests for the same key share one computation.

### Queue

```scala
Queue.bounded[A](capacity)    // offer suspends when full — back-pressure
Queue.dropping[A](capacity)   // new items discarded when full
Queue.sliding[A](capacity)    // oldest items discarded when full
Queue.unbounded[A]            // never suspends — memory risk
```

```scala
q.offer(a) / q.offerAll(as)
q.take / q.takeAll / q.takeUpTo(n) / q.takeBetween(min, max)
q.size / q.shutdown / q.awaitShutdown
```

`Enqueue[-A]` and `Dequeue[+A]` are the read/write halves — expose those in APIs rather than the full `Queue`.

Default to `bounded`. An unbounded queue turns back-pressure into an OOM.

### Hub

Broadcast: every subscriber receives every message. Subscription is scoped.

```scala
for {
  hub <- Hub.bounded[Event](128)
  _   <- ZIO.scoped(hub.subscribe.flatMap(_.take.flatMap(handle).forever)).fork
  _   <- hub.publish(event)
} yield ()
```

Varieties mirror `Queue`: `bounded`, `dropping`, `sliding`, `unbounded`. A bounded hub back-pressures the publisher to the slowest subscriber — usually you want `sliding` for telemetry-style fan-out.

### Semaphore

```scala
for {
  sem <- Semaphore.make(permits = 10)
  _   <- ZIO.foreachPar(urls)(u => sem.withPermit(fetch(u)))
} yield ()
```

`withPermits(n)` for weighted acquisition. Permits are released on failure and interruption.

## STM — composable atomicity

`STM[E, A]` describes a transaction; `.commit` turns it into a `ZIO`. All transactional reads/writes are atomic, consistent and isolated. If a conflict is detected, the transaction is retried automatically.

```scala
import zio.stm._

def transfer(from: TRef[Int], to: TRef[Int], amount: Int): STM[String, Unit] =
  for {
    balance <- from.get
    _       <- STM.fail("insufficient funds").when(balance < amount)
    _       <- from.update(_ - amount)
    _       <- to.update(_ + amount)
  } yield ()

transfer(a, b, 100).commit
```

`STM.retry` suspends the transaction until one of the `TRef`s it read changes — this is how you build blocking queues without polling:

```scala
def take[A](q: TQueue[A]): STM[Nothing, A] =
  q.poll.flatMap {
    case Some(a) => STM.succeed(a)
    case None    => STM.retry
  }
```

Data structures: `TRef`, `TArray`, `TMap`, `TSet`, `TQueue`, `TPriorityQueue`, `THub`, `TPromise`, `TSemaphore`, `TReentrantLock`.

**Limitations — important:**
- No effects inside a transaction. `STM` cannot run `ZIO`; a transaction may be retried many times, so side effects would be duplicated. Do effects before or after `.commit`.
- Performance degrades under high contention on a single `TRef`. Shard hot state.
- Large transactions are more likely to conflict and retry. Keep them small.
- Use STM when you need atomicity *across* structures; a single `Ref.modify` is cheaper for a single value.

## Schedule

`Schedule[Env, In, Out]` consumes values of type `In` and decides whether to continue, with a delay, producing `Out`.

```scala
Schedule.recurs(5)
Schedule.spaced(1.second)
Schedule.fixed(1.second)          // fixed rate, compensates for run duration
Schedule.exponential(10.millis, factor = 2.0)
Schedule.fibonacci(10.millis)
Schedule.forever
Schedule.once
Schedule.stop
Schedule.windowed(1.minute)
Schedule.cron("0 0 * * *")        // zio-cron / Schedule.cron in recent versions
```

Composition — this is the real power:

```scala
s1 && s2      // intersection: continue while BOTH continue; max of delays
s1 || s2      // union: continue while EITHER continues; min of delays
s1 ++ s2      // sequential: run s1 to completion, then s2
s1 <||> s2    // andThenEither
s.jittered                        // randomize delay — always do this for retries
s.whileInput(pred) / untilInput(pred)
s.whileOutput(pred)
s.mapZIO(o => ZIO.logInfo(s"attempt $o"))
s.tapOutput(...)
s.modifyDelay((_, d) => d.min(30.seconds))   // cap backoff
```

Canonical production retry policy:

```scala
val retryPolicy =
  Schedule.exponential(100.millis).jittered &&
  Schedule.recurs(5) &&
  Schedule.upTo(30.seconds)

apiCall.timeout(5.seconds).someOrFail(Timeout).retry(retryPolicy)
```

`retry` feeds the **error** into the schedule; `repeat` feeds the **success value**.

## Thread pools

- Default pool: async/CPU-bound work. Never block on it.
- Blocking pool: `ZIO.blocking(effect)`, `ZIO.attemptBlocking`, `ZIO.attemptBlockingInterrupt`.
- Pin to a specific executor: `effect.onExecutor(myExecutor)` / `ZIO.blockingExecutor`.
- `Runtime.setExecutor` / `setBlockingExecutor` in `bootstrap` to configure globally.

If you see latency spikes under load, the first thing to check is whether something blocking escaped onto the async pool.

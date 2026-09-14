# Error Handling & Debugging

## The three failure channels

| Channel | Meaning | In the type? | Created by |
|---|---|---|---|
| Failure (`E`) | expected, recoverable | yes | `ZIO.fail` |
| Defect | bug, unrecoverable | no | `ZIO.die`, throw inside `succeed`, `orDie` |
| Interruption | fiber cancelled | no | `fiber.interrupt`, `race`, `timeout` |

The distinction is a *design decision*, not a property of the exception. `FileNotFoundException` is a recoverable failure when the user typed a path, and a defect when the file is a bundled resource that must exist.

## Cause

`Cause[E]` is a tree, not a single value — ZIO can fail for several reasons at once (parallel effects, a failure plus a finalizer defect).

```scala
sealed trait Cause[+E]
// Empty | Fail(e) | Die(t) | Interrupt(fiberId)
// Then(left, right)   -- sequential composition (e.g. failure + finalizer failure)
// Both(left, right)   -- parallel composition  (e.g. two parallel failures)
```

Useful accessors:

```scala
cause.failures        // List[E]
cause.defects         // List[Throwable]
cause.failureOption   // Option[E]
cause.isInterrupted
cause.interruptOption
cause.squash          // best-effort single Throwable
cause.prettyPrint     // rendered tree with execution trace
```

## Exit

```scala
sealed trait Exit[+E, +A]
// Success(a) | Failure(cause: Cause[E])
```

Returned by `fiber.await`, `effect.exit`, and passed to `onExit`/`acquireReleaseExit` finalizers — use it when cleanup must branch on *how* the effect ended.

## Operator map

```scala
// recover
effect.catchAll(e => fallbackZIO)
effect.catchSome { case Timeout => retryZIO }
effect.catchAllCause(c => ZIO.logErrorCause(c) *> fallback)   // sees defects & interruption
effect.catchAllDefect(t => ...)
effect.orElse(other)             // on any typed failure
effect.orElseSucceed(default)
effect.fold(e => -1, a => a)
effect.foldZIO(e => logAndFail(e), a => ZIO.succeed(a))
effect.foldCauseZIO(c => ..., a => ...)

// convert
effect.either                    // ZIO[R, Nothing, Either[E, A]]
effect.absolve                   // inverse of .either
effect.option                    // ZIO[R, Nothing, Option[A]]
effect.exit                      // ZIO[R, Nothing, Exit[E, A]]
effect.flip                      // swap E and A — handy in tests
effect.merge                     // E <: A  => ZIO[R, Nothing, A]

// escalate / narrow
effect.orDie                     // E <: Throwable -> defect
effect.orDieWith(f)
effect.refineToOrDie[SQLException]
effect.refineOrDie { case e: IOException => e }
effect.unrefineTo[Throwable]     // defects back into E
effect.resurrect

// translate
effect.mapError(RepoError.from)
effect.mapErrorCause(_.map(RepoError.from))
effect.sandbox                   // ZIO[R, Cause[E], A]
effect.unsandbox

// observe without handling
effect.tapError(e => ZIO.logWarning(e.toString))
effect.tapErrorCause(ZIO.logErrorCause(_))
effect.tapDefect(...)
effect.onError(cause => cleanup)
```

## Accumulating errors

`ZIO.foreach` fails fast on the first error. To collect all of them:

```scala
ZIO.validate(inputs)(validate)        // ZIO[R, ::[E], List[A]] — accumulates
ZIO.validatePar(inputs)(validate)
ZIO.validateDiscard / validateFirst
a.validate(b)                         // zip that accumulates both errors
```

Use this for form/DTO validation where reporting one error at a time is bad UX.

## Combining effects with different error types

Widening to the LUB often lands you at `Any` or `Throwable`. Two ways out:

1. Define a sealed trait for the module's errors and `mapError` each source into it at the boundary.
2. Use `Either`-shaped composition (`.either`) and reassemble deliberately.

```scala
sealed trait CheckoutError
object CheckoutError {
  final case class Payment(cause: PaymentError)   extends CheckoutError
  final case class Inventory(cause: StockError)   extends CheckoutError
  case object CartEmpty                           extends CheckoutError
}

val checkout =
  for {
    _ <- ensureNonEmpty.mapError(_ => CheckoutError.CartEmpty)
    _ <- reserveStock.mapError(CheckoutError.Inventory(_))
    _ <- charge.mapError(CheckoutError.Payment(_))
  } yield ()
```

## Retries

```scala
effect.retry(Schedule.recurs(3))
effect.retry(Schedule.exponential(100.millis).jittered && Schedule.recurs(5))
effect.retryOrElse(schedule, (e, _) => fallback)
effect.retryWhile(_.isTransient)
effect.retryUntil(_.isFatal)
```

Retry only what is *transient*. Retrying a 400 Bad Request is a bug. Encode retryability into the error ADT (`def retryable: Boolean`) and gate with `retryWhile`.

Always pair a retry with a `timeout` — otherwise a hung call retries a hung call.

## Debugging

```scala
effect.debug("label")                 // print value/error with a tag
ZIO.logDebug("...") / logInfo / logWarning / logError / logErrorCause
ZIO.logSpan("db-query")(effect)       // adds elapsed time to log records
ZIO.logAnnotate("requestId", id)(effect)
```

Runtime configuration for diagnostics (in `bootstrap`):

```scala
override val bootstrap =
  Runtime.enableCurrentFiber ++
  Runtime.enableRuntimeMetrics ++
  Runtime.setLogLevel(LogLevel.Debug)
```

**Execution traces.** ZIO builds an async stack trace across fiber boundaries automatically. `cause.prettyPrint` shows where the effect was *defined*, which is usually more useful than the JVM stack.

**Fiber dumps.** For a stuck application, dump all live fibers and their state:

```scala
Fiber.dumpAll.flatMap(ZIO.foreachDiscard(_)(_.dump.flatMap(d => Console.printLine(d.prettyPrint))))
```

On the JVM, `Ctrl+\` (SIGQUIT) triggers a fiber dump on a running ZIO app.

**Supervision.** `effect.supervised(supervisor)` or a custom `Supervisor[A]` lets you observe fiber start/end for diagnostics and leak hunting.

## Best practices

- One sealed error ADT per module/bounded context. Flat, few cases, no inheritance chains.
- Errors carry data, not prose. `UserNotFound(id)`, not `Error("user 42 not found")`. Render messages at the edge.
- Translate at boundaries: infrastructure exception → domain error in the adapter; domain error → HTTP status in the route.
- Defects are for *programmer* errors. If an operator would recover from it, it's a typed error.
- Never log and rethrow the same failure at every layer — log once, where it's handled.
- In tests, assert on the error value (`effect.flip`), not on a message string.

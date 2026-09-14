---
name: zio
description: Write, review, test, refactor and harden Scala code built on ZIO 2.x — effects, typed errors, fibers, ZLayer dependency injection, Scope, STM, ZStream and the ZIO ecosystem (zio-http, zio-kafka, zio-config, zio-schema, zio-json, zio-test, zio-grpc, zio-telemetry). Use this whenever the user works with Scala code that imports `zio._`, mentions ZIO, ZIO 2, ZLayer, ZStream, ZIOAppDefault, zio-test, or any `dev.zio` library — and also when they ask to design, debug, benchmark, secure or add a library to a ZIO application.
allowed-tools: Read, Write, Edit, Glob, Grep, Bash(sbt:*), Bash(scala-cli:*), Bash(mill:*), Bash(git diff:*), Bash(git status:*), WebFetch
model: inherit
# ^ real design/review work; leave on the session model. Use `sonnet` if you want to pin it cheaper.
---

# ZIO (Scala)

Authoritative reference for building production Scala applications on **ZIO 2.x**.
Grounded in *Zionomicon* (De Goes, Fraser, Khajavi) and the `github.com/zio` ecosystem.

## How to use this skill

1. **Detect the version first.** Read `build.sbt` / `build.sc` / `project/*.scala` for the ZIO version and Scala version (2.13 vs 3). Everything here is ZIO 2.x; ZIO 1.x had a different environment/layer API (`Has[_]`, `ZIO.accessM`, `ZManaged`) — if you see those, you are on 1.x, say so and offer a migration path.
2. **Never invent library versions.** Check the version already in the build. If a new dependency is needed, look it up (`https://index.scala-lang.org/zio/<repo>`) or ask — do not guess a version string.
3. **Load only the reference you need.** The core model is below; per-library details live in `references/` (index at the bottom).
4. **Make it compile.** After non-trivial edits, run `sbt compile` / `sbt Test/compile` (or `scala-cli compile .`). ZIO leans hard on inference; a change that "looks right" often doesn't typecheck.

---

## 1. The core model

`ZIO[R, E, A]` is an immutable **blueprint** describing a workflow that needs an environment `R`, may fail with `E`, and may succeed with `A`. Constructing it does nothing; only the runtime executes it.

| Alias | Expands to | Use for |
|---|---|---|
| `UIO[A]` | `ZIO[Any, Nothing, A]` | cannot fail |
| `URIO[R, A]` | `ZIO[R, Nothing, A]` | needs `R`, cannot fail |
| `Task[A]` | `ZIO[Any, Throwable, A]` | Java/legacy interop |
| `RIO[R, A]` | `ZIO[R, Throwable, A]` | needs `R`, throws |
| `IO[E, A]` | `ZIO[Any, E, A]` | typed domain error |

Variance: `R` is contravariant, `E` and `A` covariant. Combining two effects widens `E`/`A` to their least upper bound and narrows `R` to the intersection.

### Constructors — pick the right one

```scala
ZIO.succeed(42)                       // pure value, no side effect
ZIO.fail(UserNotFound(id))            // typed, recoverable failure
ZIO.attempt(sideEffectingCall())      // side-effecting, may throw -> Task
ZIO.attemptBlocking(jdbcCall())       // blocking; runs on the blocking pool
ZIO.attemptBlockingInterrupt(c)       // blocking + interruptible via Thread.interrupt
ZIO.fromFuture(implicit ec => f)      // Scala Future — already running; not lazy
ZIO.async[Any, E, A](cb => ...)       // callback-based async APIs
ZIO.fromEither / fromOption / fromTry // lift existing values
ZIO.die(new IllegalStateException)    // unrecoverable defect
ZIO.suspendSucceed(expensiveBlueprint)// defer blueprint construction
```

**Rule:** any code with a side effect must be wrapped in `ZIO.attempt`/`succeed` *inside* the effect, never evaluated when the blueprint is built. `ZIO.succeed(println("x"))` is a bug waiting to happen — use `ZIO.attempt` or `Console.printLine`.

### Composition

```scala
for {
  name  <- Console.readLine
  _     <- Console.printLine(s"hi $name")
  users <- ZIO.foreachPar(ids)(fetchUser)      // parallel, collects results
  _     <- ZIO.foreachDiscard(users)(persist)  // sequential, drops results
} yield ()
```

Key operators: `map`, `flatMap`, `zip`/`<*>`, `zipRight`/`*>`, `zipLeft`/`<*`, `zipPar`, `race`, `orElse`, `either`, `fold`/`foldZIO`, `when`/`unless`, `tap`/`tapError`, `timeout`, `retry`, `repeat`, `forever`, `ensuring`.

`ZIO.foreach` is the effectful `map` over a collection. Use `ZIO.foreachPar` for parallelism and `withParallelism(n)` to bound it.

### Entry point

```scala
object Main extends ZIOAppDefault {
  def run = program.provide(AppLayers.all)
}
```

`ZIOAppDefault` supplies the default services (`Clock`, `Console`, `Random`, `System`) and a runtime. Override `bootstrap` to change the runtime configuration (logger, config provider, metrics); it runs before anything else in the app.

---

## 2. Error model — the thing people get wrong

Three distinct failure channels:

- **Typed error (`E`)** — an expected, recoverable domain failure. Appears in the type signature.
- **Defect** — a bug or unrecoverable condition. Not in the type signature. Produced by `ZIO.die`, a throw inside `succeed`, or `orDie`.
- **Interruption** — a fiber was cancelled. Tracked in `Cause`, not in `E`.

`Cause[E]` is the full failure tree (can hold multiple failures, defects, and interruptions). `Exit[E, A]` is `Success(a) | Failure(Cause[E])`.

```scala
effect.catchAll(e => fallback)              // handle typed errors
effect.catchSome { case NotFound(_) => ... }
effect.catchAllCause(cause => ...)          // also sees defects + interruption
effect.orDie                                // E becomes a defect (E <: Throwable)
effect.orDieWith(e => new RuntimeException(e.toString))
effect.refineToOrDie[SQLException]          // narrow a Throwable to what you care about
effect.mapError(DomainError.from)           // translate at the boundary
effect.sandbox / .unsandbox                 // expose Cause in E and back
```

**House rules**
- Model recoverable failures as a sealed trait, not `Throwable`. `ZIO[R, UserServiceError, User]` documents itself.
- Convert to defects at the point where recovery is impossible, not everywhere.
- Never `catchAll(_ => ZIO.unit)` — you've silently swallowed a failure. At minimum `tapError(e => ZIO.logError(...))`.
- Don't put `Throwable` in a service interface just because the implementation uses JDBC. Translate at the adapter boundary.

Deeper coverage: `references/error-handling.md`.

---

## 3. Dependency injection — the Service Pattern

This is the backbone of any real ZIO app. Five steps, always in this order:

```scala
// 1. trait describes capabilities — environment type is always `Any` here
trait UserRepo {
  def find(id: UserId): IO[RepoError, Option[User]]
  def save(user: User): IO[RepoError, Unit]
}

// 2. concrete implementation as a case class
// 3. dependencies arrive via the CONSTRUCTOR, never via the environment
final case class UserRepoLive(pool: ConnectionPool, metrics: Metrics) extends UserRepo {
  def find(id: UserId): IO[RepoError, Option[User]] = ???
  def save(user: User): IO[RepoError, Unit]         = ???
}

// 4. layer in the companion of the implementation
object UserRepoLive {
  val layer: ZLayer[ConnectionPool with Metrics, Nothing, UserRepo] =
    ZLayer.fromFunction(UserRepoLive(_, _))
}

// 5. accessors in the companion of the TRAIT, for ergonomics
object UserRepo {
  def find(id: UserId): ZIO[UserRepo, RepoError, Option[User]] =
    ZIO.serviceWithZIO(_.find(id))
  def save(user: User): ZIO[UserRepo, RepoError, Unit] =
    ZIO.serviceWithZIO(_.save(user))
}
```

**Why constructor and not environment:** the trait defines *what* the service does, not what it needs. Keep `R = Any` in trait method signatures; put dependencies in the implementation's constructor. This is the single most common design mistake in ZIO codebases.

### Building layers

```scala
ZLayer.fromFunction(ServiceLive(_, _))     // no init/teardown — preferred
ZLayer { for { c <- ZIO.service[Config]; s <- ZIO.succeed(Live(c)); _ <- s.start } yield s }
ZLayer.scoped { ... ZIO.addFinalizer(s.shutdown) ... }   // needs teardown
ZLayer.succeed(StaticConfig(8080))         // constant value
```

### Wiring

```scala
program.provide(               // order doesn't matter; ZIO wires the graph
  UserRepoLive.layer,
  ConnectionPoolLive.layer,
  MetricsLive.layer,
  ZLayer.Debug.mermaid         // optional: prints the dependency graph
)

program.provideSome[Clock](A.layer, B.layer)   // defer part of the graph (tests!)
val bundle = ZLayer.make[UserRepo](UserRepoLive.layer, ConnectionPoolLive.layer)
```

Guarantees: layers are acquired in parallel where possible, **memoized** (a layer appearing twice in the graph is built once), and released in reverse order when the workflow ends by success, failure or interruption.

Missing dependency = compile error listing exactly what's absent. Trust it.

Advanced (multiple instances of one type, error handling in construction, `ZEnvironment` internals): `references/dependency-injection.md`.

---

## 4. Resources — `Scope`

```scala
ZIO.acquireRelease(open(f))(f => close(f).orDie)     // -> ZIO[Scope, E, A]
ZIO.acquireReleaseWith(acquire)(release)(use)        // scoped to `use` only
effect.ensuring(cleanup)                             // finalizer, no acquisition
ZIO.scoped { scopedEffect }                          // discharge the Scope requirement
```

`Scope` in the environment type means "this workflow has resources that need closing". `ZIO.scoped` closes them. Finalizers run on success, failure *and* interruption — `try/finally` does not survive async boundaries, `acquireRelease` does.

Acquisition is uninterruptible by default; `use` is interruptible. Never do heavy work in `acquire`.

Details incl. child scopes and `Reloadable`: `references/resources-and-scope.md`.

---

## 5. Concurrency essentials

- **Fiber** — lightweight, interruptible green thread. `effect.fork` → `Fiber[E, A]`, `fiber.join`, `fiber.interrupt`, `fiber.await` (returns `Exit`).
- **Structured concurrency** — a forked fiber's lifetime is bound to its parent's scope. Prefer `ZIO.foreachPar`, `raceWith`, `zipPar` over manual `fork`. Use `forkDaemon`/`forkScoped` deliberately, not by accident.
- **Interruption** is cooperative and prompt: it happens at effect boundaries. Guard critical sections with `ZIO.uninterruptible` / `.uninterruptibleMask`.

Shared state, by need:

| Need | Use |
|---|---|
| shared mutable value | `Ref` (`update`, `modify` — atomic per operation) |
| update requiring an effect | `Ref.Synchronized` |
| fiber-local value (context, tracing) | `FiberRef` |
| one-shot signal between fibers | `Promise` |
| work distribution / back-pressure | `Queue` (`bounded`, `dropping`, `sliding`) |
| broadcast to many consumers | `Hub` |
| limit concurrency | `Semaphore` (`withPermits(n)`) |
| **multi-structure atomicity** | `STM` / `TRef` |

Critical: individual `Ref` operations are atomic, but *composing* two of them is not. If you need two updates to happen atomically, either fold them into one `modify`, or use `STM`.

Retries and repetition use `Schedule`:

```scala
effect.retry(Schedule.exponential(100.millis) && Schedule.recurs(5))
effect.repeat(Schedule.spaced(1.minute))
effect.retry(Schedule.exponential(10.millis).jittered.whileInput[TransientError](_.retryable))
```

Full treatment (STM data structures, supervision, schedule algebra): `references/concurrency.md`.

---

## 6. Configuration

```scala
final case class ServerConfig(host: String, port: Int, secret: Config.Secret)
object ServerConfig {
  implicit val config: Config[ServerConfig] =
    (Config.string("host") zip
     Config.int("port").withDefault(8080).validate("port must be 1..65535")(p => p > 0 && p < 65536) zip
     Config.secret("secret"))
      .map { case (h, p, s) => ServerConfig(h, p, s) }
      .nested("server")
}

val cfg = ZIO.config[ServerConfig]
```

Use `Config.Secret` for anything sensitive — it redacts in `toString`, compares in constant time and can be wiped. Default provider is env vars then system properties; override in `bootstrap` with `Runtime.setConfigProvider(...)`. HOCON/YAML need `zio-config-typesafe` / `zio-config-yaml`.

More: `references/config.md`.

---

## 7. Testing

```scala
import zio._, zio.test._

object UserServiceSpec extends ZIOSpecDefault {
  def spec = suite("UserService")(
    test("creates a user") {
      for {
        id <- UserService.create("ada")
        u  <- UserService.find(id)
      } yield assertTrue(u.exists(_.name == "ada"))
    },
    test("fails on duplicate") {
      UserService.create("ada").flip.map(e => assertTrue(e == Duplicate("ada")))
    } @@ TestAspect.timeout(5.seconds)
  ).provide(UserServiceLive.layer, UserRepoInMemory.layer)
}
```

- Tests **are** effects; no `unsafeRun` in test bodies.
- Prefer `assertTrue` (smart assertions) — it works for pure and effectful code and produces readable diffs.
- The default `TestEnvironment` gives deterministic `TestClock`, `TestRandom`, `TestConsole`, `TestSystem`. **Time does not pass unless you advance it**: `TestClock.adjust(1.hour)`. A test on `ZIO.sleep` that hangs almost always means a missing `adjust` (fork the effect first, then adjust).
- Aspects compose behaviour: `@@ TestAspect.nonFlaky`, `@@ timeout`, `@@ sequential`, `@@ withLiveClock`, `@@ ignore`, `@@ jvmOnly`.
- Property-based testing is first class: `check(Gen.int, Gen.alphaNumericString) { (i, s) => ... }`.

Deeper (custom aspects, shared layers across suites, shrinking, mocking): `references/testing.md`.

---

## 8. Review checklist — anti-patterns to flag

When reading or reviewing ZIO code, actively look for these:

- `Unsafe.unsafe { runtime.unsafe.run(...) }` anywhere except the true edge (main, a framework callback). Running an effect inside another effect breaks interruption and error tracking.
- Side effects in `ZIO.succeed` instead of `ZIO.attempt`.
- Blocking calls (JDBC, `Thread.sleep`, file IO, `Await.result`) not wrapped in `ZIO.attemptBlocking` — they starve the async thread pool.
- `Throwable` as the error type of a domain service.
- `.orDie` / `catchAll(_ => ZIO.unit)` used to make the compiler quiet.
- Dependencies declared in the trait's environment type instead of the implementation's constructor.
- `fork` without a matching `join`/`interrupt`, or `forkDaemon` used to dodge structured concurrency.
- Two `Ref` operations that need to be atomic together.
- Unbounded `Queue.unbounded` / `ZStream.buffer(Int.MaxValue)` where back-pressure was the point.
- Resources acquired with `ZIO.attempt` and closed with `ensuring` instead of `acquireRelease`.
- `ZStream` collected with `runCollect` on an unbounded source.
- `var` or mutable collections shared between fibers.
- Tests using the real clock (`Thread.sleep`, `@@ withLiveClock` everywhere) instead of `TestClock`.

## 9. Security checklist

- Secrets: `Config.Secret`, never plain `String`; never log the config object; verify custom loggers don't serialize it.
- Never interpolate user input into SQL — parameterize (`zio-jdbc` `sql"..."` interpolator is safe by design).
- HTTP: `sandbox`/`handleError` every route so internal errors never leak stack traces to clients; set explicit CORS, CSRF and auth middleware; validate bodies with `zio-schema` rather than hand-parsing.
- Don't let a typed error's `toString` become the HTTP response body — map errors to deliberate status codes and messages.
- Bound every external call: `timeout`, a retry `Schedule` with `jittered`, and a `Semaphore` or bounded `Queue` to cap concurrency.
- Log with `ZIO.logAnnotate` for correlation IDs; scrub PII before it reaches a logger.
- Dependency hygiene: pin versions, run `sbt dependencyCheck`/`scala-steward`, avoid unmaintained `dev.zio` community modules.

---

## Reference index

Read the file that matches the task. Do not load them all.

| File | Covers |
|---|---|
| `references/error-handling.md` | Cause, Exit, defects, retries, debugging, fiber dumps, best practices |
| `references/concurrency.md` | Fibers, interruption, Ref/Promise/Queue/Hub/Semaphore, STM, Schedule algebra |
| `references/dependency-injection.md` | ZLayer in depth, ZEnvironment, memoization, multiple instances, derivation |
| `references/resources-and-scope.md` | acquireRelease, Scope, child scopes, scoped layers |
| `references/streams.md` | ZStream, ZSink, ZPipeline, ZChannel — construction, transformation, flow control |
| `references/testing.md` | zio-test: assertions, aspects, TestEnvironment, property-based testing, mocks |
| `references/http.md` | zio-http: routes, handlers, middleware, endpoints, client, WebSockets |
| `references/kafka.md` | zio-kafka: consumers, producers, offsets, rebalancing, transactions |
| `references/config.md` | zio-config: descriptors, providers, HOCON/YAML, secrets, validation |

**Not written yet.** For these, work from the core model above plus the official docs at
`https://zio.dev/<library>`: zio-schema, zio-json, zio-logging and metrics/telemetry,
persistence (zio-jdbc, Quill, zio-sql), zio-grpc, and interop (Java, `Future`,
cats-effect, Pekko, Scala.js).

**Adding a library not listed here:** follow the same shape — a new `references/<lib>.md` with dependency coordinates, the 5–10 core types, a minimal working example, integration with ZLayer, testing approach, and the library's own gotchas. Then add a row to this table.

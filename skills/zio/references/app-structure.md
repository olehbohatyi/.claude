# Application Structure

How a ZIO application is assembled: entry point, bootstrap, layer wiring, graceful
shutdown, and module layout.

This file is more opinionated than the others — these are defaults that work, not the
only correct answers. When a repo already has a convention, follow the repo.

## Entry point

```scala
object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j >>>
      Runtime.setConfigProvider(ConfigProvider.fromHoconFile(configFile))

  def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    program.provide(
      ApiLive.layer,
      UserServiceLive.layer,
      UserRepoLive.layer,
      poolLayer,
      Server.default
    )
```

**`bootstrap` vs `run`.** `bootstrap` configures the *runtime* — loggers, config provider,
metrics, fatal error handling, supervisors — and is applied before anything else
executes. Everything else belongs in `run`. Putting logger setup inside `run` means the
first effects of your application log with the default logger.

`ZIOAppDefault` supplies `Clock`, `Console`, `Random`, `System` and a `Scope`. Use
`ZIOApp` instead only when you need a custom runtime shared between several apps.

Multiple entry points (an API and a worker) are ordinary: each gets its own `object ...
extends ZIOAppDefault`, sharing the same service layers. That's a strong argument for
keeping layers in companion objects rather than in one `Main`.

## Layer wiring

Provide everything at the top, once. Compositional detail belongs in the layers
themselves, not at the call site.

```scala
object AppLayers:
  val live: ZLayer[Any, Throwable, Api & UserService] =
    ZLayer.make[Api & UserService](
      ApiLive.layer,
      UserServiceLive.layer,
      UserRepoLive.layer,
      poolLayer
    )
```

- `.provide(...)` at the application edge; **never** inside a service. A service that
  provides its own dependencies can't be tested with substitutes, which is the whole
  point of the pattern.
- Order in the list is irrelevant. ZIO builds the graph and reports what's missing at
  compile time.
- Layers are memoized: a `ConnectionPool` needed by three repositories is built once.
- `ZLayer.Debug.tree` / `ZLayer.Debug.mermaid` in the list when a wiring error is opaque.
- `.provideSome[Dep](...)` to leave one slot open — the standard way to build a test
  harness that swaps exactly one implementation.

Keep a `live` and a `test` set of layers side by side. If building the test wiring
requires changing production code, the dependency graph is wrong.

## Module layout

For a single-service repo, layout by **domain**, not by technical role:

```
src/main/scala/com/acme/
├── Main.scala
├── config/          AppConfig + Config descriptors
├── domain/          pure types, no ZIO: User, Order, DomainError
├── service/         business logic: trait + Live + layer
├── repo/            persistence: trait + Live + layer
├── api/             zio-http routes, request/response DTOs, error mapping
└── consumer/        Kafka consumers and handlers
```

`domain` must not depend on anything else. A domain type that imports `zio.http` or
`zio.jdbc` has stopped being a domain type.

For a multi-module sbt build, the same split becomes modules with enforced dependency
direction:

```
core      -> domain types, no dependencies
service   -> depends on core
adapters  -> depends on core (jdbc, http client, kafka)
app       -> depends on all, contains Main and the wiring
```

The value is that the compiler enforces the arrows. A single module relies on discipline
alone.

### The onion

Dependencies point inward: `api` and `consumer` (edges) depend on `service`, which
depends on `repo` traits, which are implemented at the edge. The domain is the center and
depends on nothing. In ZIO the traits-plus-layer pattern is what makes this mechanical —
`UserService` depends on the `UserRepo` *trait*; `UserRepoLive` is supplied at wiring
time.

## DTOs vs domain types

Keep the types you serialize separate from the types you reason about, wherever the two
evolve on different schedules. A JSON codec derived directly on a domain case class means
any refactor is a public API change (`references/schema-and-json.md`).

For a small internal service, one set of types is fine and the duplication isn't worth
it. For anything with external consumers or long-retention Kafka topics, split them.

## Graceful shutdown

```scala
def run =
  (for
    _ <- ZIO.logInfo("starting")
    _ <- server.forever
  yield ()).provide(layers)
```

What ZIO gives you for free: on SIGINT/SIGTERM the main fiber is interrupted, which
interrupts its children (structured concurrency), which runs finalizers in reverse
acquisition order, which closes every scoped layer. Correct shutdown is mostly a
consequence of using `ZLayer.scoped` and `.fork` rather than `.forkDaemon`.

What you have to do deliberately:

- **In-flight work.** A Kafka consumer should stop fetching and drain what it holds
  before closing — zio-kafka's `*StreamWithControl` variants plus
  `Consumer.runWithGracefulShutdown` exist for this.
- **Drain timeouts.** Wrap shutdown work in `.timeout` so a stuck finalizer can't block
  the process forever.
- **Health/readiness.** Flip readiness to false at the start of shutdown so the load
  balancer stops sending traffic before you stop accepting it.
- `.forkDaemon` fibers are outside supervision and will not be interrupted. Every one is
  a shutdown bug unless something explicitly interrupts it.

## Health checks

Liveness and readiness are different questions. Liveness: the process is running.
Readiness: dependencies are reachable *and* we are not shutting down. Implement readiness
as a `Ref[Boolean]` the shutdown hook flips, ANDed with a cheap dependency probe — never
by running a real query on every poll.

## Failure at startup

A layer that fails to build fails the application, which is correct: a service that can't
reach its database should not start and report healthy. Don't `.orDie` startup errors
into oblivion, and don't retry forever — a bounded, jittered retry followed by a clean
exit gives the orchestrator a chance to do its job.

Validate config at load time (`references/config.md`) so a bad value fails here rather
than on the first request that reads it.

## Checklist

- Runtime configuration in `bootstrap`; application logic in `run`
- Layers provided once, at the top; never inside a service
- `live` and `test` layer sets, with no production code changes needed to build the test one
- `domain` package/module free of ZIO ecosystem imports
- Dependencies declared as constructor parameters, per the service pattern
- Resources acquired via `ZLayer.scoped` so shutdown is automatic
- No `.forkDaemon` without an explicit interrupt path
- Drain and shutdown paths bounded by `.timeout`
- Readiness flipped before the server stops accepting
- Startup failures surfaced, not swallowed

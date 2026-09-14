# Dependency Injection (ZLayer) — Advanced

Prerequisite: the Service Pattern in `SKILL.md` §3.

## ZEnvironment

`ZEnvironment[R]` is a typed, heterogeneous map from service types to instances, keyed by `Tag[A]` (materialized by a macro from the static type). `ZIO[R, E, A]` is roughly `ZEnvironment[R] => ...`.

```scala
ZIO.service[UserRepo]                  // fetch a service
ZIO.serviceWith[Config](_.port)        // fetch and map
ZIO.serviceWithZIO[UserRepo](_.find(id))
ZIO.environment[R]                     // whole environment
ZIO.environmentWith[R](_.get[UserRepo])
effect.provideEnvironment(ZEnvironment(repoImpl))
```

Because tags are structural, `ZEnvironment[A with B]` genuinely holds both. On Scala 3, tags cannot be materialized for intersection types in covariant position — if you hit that error (common with `sttp`'s `SttpClient` alias), use a concrete single type instead.

## Layer constructors

```scala
ZLayer.succeed(value)                          // constant
ZLayer.fromFunction(ServiceLive(_, _))         // pure construction from deps
ZLayer.fromZIO(effect)                         // effectful, no finalizer
ZLayer(effectualForComprehension)              // same, nicer syntax
ZLayer.scoped { ... }                          // with finalizer
ZLayer.fromFunctionZIO(deps => effect)
ZLayer.service[A]                              // pass a service through
ZLayer.environment[R]
ZLayer.empty
```

Typical scoped layer with init and teardown:

```scala
object ConnectionPoolLive {
  val layer: ZLayer[DbConfig, Throwable, ConnectionPool] =
    ZLayer.scoped {
      for {
        cfg  <- ZIO.service[DbConfig]
        pool <- ZIO.acquireRelease(open(cfg))(p => close(p).orDie)
      } yield ConnectionPoolLive(pool)
    }
}
```

Prefer `acquireRelease` over `addFinalizer` when the resource has a distinct acquisition step — it guarantees the finalizer is registered atomically with acquisition.

## Composition

```scala
a ++ b            // horizontal: both outputs, combined inputs
a >>> b           // vertical: a's output feeds b's input
a >+> b           // vertical, keeping a's output too
ZLayer.make[Out](l1, l2, l3)       // automatic wiring for a target type
ZLayer.makeSome[In, Out](l1, l2)   // wiring, leaving `In` to be supplied later
layer.orDie / layer.orElse(fallback) / layer.mapError(f)
layer.memoize                       // ZIO[Scope, Nothing, ZLayer[...]] — explicit sharing
layer.fresh                         // opt OUT of memoization
layer.passthrough                   // output includes the inputs
layer.project(f)                    // narrow the produced service
ZLayer.Debug.tree / ZLayer.Debug.mermaid   // print / link the dependency graph
```

## Memoization

Within a single `provide`/`make` call, a layer is built **once** no matter how many times it appears in the graph. This is what makes a shared connection pool work.

Memoization is per wiring call. Two separate `provide` calls build two pools. If you need one instance across several independent workflows, build it once and pass it down, or use `layer.memoize` inside a `ZIO.scoped`.

`layer.fresh` forces a new instance — useful when a test needs isolation from a shared layer.

## Errors during construction

```scala
val layer: ZLayer[Any, ConfigError, Db] = ZLayer { loadConfig.flatMap(connect) }

layer.orDie                                    // fail fast, treat as unrecoverable
layer.catchAll(e => ZLayer.succeed(fallbackDb))
layer.retry(Schedule.exponential(1.second) && Schedule.recurs(5))   // wait for a DB to come up
```

`ZIOAppDefault.run` accepts a failing layer; the error surfaces at startup with a full cause. For services that may be temporarily unavailable, a retry schedule on the layer is the idiomatic startup backoff.

## Multiple services of the same type

Tags are by type, so `ZEnvironment` holds one instance per type. To hold two, wrap them:

```scala
final case class Primary(repo: UserRepo)
final case class Replica(repo: UserRepo)

val layers = ZLayer.succeed(Primary(p)) ++ ZLayer.succeed(Replica(r))
ZIO.serviceWith[Primary](_.repo)
```

Opaque types (Scala 3) or tagged newtypes work equally well. Don't reach for `ZLayer` tricks here — a wrapper case class is clearer.

## Automatic derivation

With `zio-macros` / ZIO 2.1's derivation support you can generate accessors and layers:

```scala
import zio.macros.accessible

@accessible trait UserRepo { ... }   // generates the companion accessors
```

`ZLayer.derive[ServiceLive]` (ZIO 2.1+) builds a layer from the primary constructor, resolving each parameter from the environment, `Config` values, or defaults. Convenient, but the explicit `ZLayer.fromFunction` is easier to read in a review — use derivation for large service graphs, not for three-field services.

## Organizing a real application

```scala
object AppLayers {
  val infra: ZLayer[Any, Throwable, ConnectionPool & HttpClient & Metrics] =
    ZLayer.make[ConnectionPool & HttpClient & Metrics](
      ConnectionPoolLive.layer, HttpClientLive.layer, MetricsLive.layer,
      AppConfig.layer
    )

  val domain: ZLayer[ConnectionPool & HttpClient, Nothing, UserService & OrderService] =
    ZLayer.make[UserService & OrderService](
      UserServiceLive.layer, UserRepoLive.layer,
      OrderServiceLive.layer, OrderRepoLive.layer
    )

  val all = infra >+> domain
}

object Main extends ZIOAppDefault {
  def run = HttpServer.start.provide(AppLayers.all, Server.default)
}
```

Group by architectural ring (config → infrastructure → repositories → domain services → API), not alphabetically. Keep `Main` to a single `provide`.

## Testing with layers

```scala
def spec = suite("OrderService")(
  test("..." ) { ... }
).provide(
  OrderServiceLive.layer,
  UserRepoInMemory.layer,        // swap one implementation
  PaymentGatewayStub.layer
)

// share an expensive layer across all tests in a suite
).provideShared(PostgresContainer.layer)
```

`provideSome[TestEnvironment]` when tests still need `TestClock`/`TestRandom`. Use `@@ TestAspect.sequential` with `provideShared` if the shared resource isn't safe for concurrent tests.

## Environment vs constructor — the rule again

| Where | What goes there |
|---|---|
| Trait method signature | `R = Any`. The interface must not know about dependencies. |
| Implementation constructor | every dependency the implementation needs |
| ZIO environment | only services the *caller* is composing against, i.e. the top-level program's requirements |

The exception is genuinely *contextual* data — a request-scoped correlation ID, a transaction handle, regional settings. Those legitimately belong in the environment, because they change per call rather than per wiring. See the "contextual data types" pattern: `ZIO.provideSomeLayer[R](ZLayer.succeed(ctx))` around a sub-workflow.

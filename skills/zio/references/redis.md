# ZIO Redis

```scala
"dev.zio" %% "zio-redis"           % V
"dev.zio" %% "zio-schema-protobuf" % V   // supplies the binary codec
"dev.zio" %% "zio-redis-embedded"  % V % Test
```

A ZIO-native Redis client — no Lettuce or Jedis underneath. Commands fail with a typed
`RedisError`, not `Throwable`.

Check the version at `https://index.scala-lang.org/zio/zio-redis` before copying
anything; the layer names changed during the 1.x line.

## Wiring

Redis stores bytes, so the client needs a codec to turn your types into them. That's what
`CodecSupplier` provides — one instance covers every type with a `Schema` (see
`references/schema-and-json.md`).

```scala
import zio._, zio.redis._, zio.schema._, zio.schema.codec._

object ProtobufCodecSupplier extends CodecSupplier:
  def get[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec

val app = myApp.provide(
  Redis.local,                                     // localhost:6379
  ZLayer.succeed[CodecSupplier](ProtobufCodecSupplier)
)
```

`Redis.local` is a convenience. For anything real, build the config from your own:

```scala
val redisLayer: ZLayer[Any, Throwable, Redis] =
  ZLayer.make[Redis](
    ZLayer.fromZIO(ZIO.config[RedisSettings].map(s => RedisConfig(s.host, s.port))),
    RedisExecutor.layer,
    Redis.layer,
    ZLayer.succeed[CodecSupplier](ProtobufCodecSupplier)
  )
```

The connection is pooled and scoped — it closes on shutdown like any other scoped layer.

Codec choice is a **persistence decision, not an implementation detail**: it determines
the byte layout of everything you write. Switching from protobuf to JSON orphans every
existing key. Pick one per Redis instance and treat a change as a migration.

## Commands

```scala
val program: ZIO[Redis, RedisError, Unit] =
  for
    redis <- ZIO.service[Redis]
    _     <- redis.set("session:abc", user, Some(30.minutes))
    got   <- redis.get("session:abc").returning[User]     // Option[User]
    _     <- redis.hSet("counters", ("hits", 1L))
    _     <- redis.rPush("queue", job)
    _     <- redis.sAdd("seen", id)
    _     <- redis.expire("seen", 1.hour)
  yield ()
```

The `.returning[A]` step exists because Redis is untyped: `get` names the key, `returning`
says how to decode the bytes. If `A` doesn't match what was written, you get a
`RedisError.ProtocolError` at runtime, not a compile error. That's the main sharp edge of
this library — the type safety is at the codec boundary, not across process restarts.

Commands map closely to the Redis names (`hSet`, `zAdd`, `lRange`, `incrBy`, `scan`), so
the Redis documentation is directly usable.

## Patterns

### Cache-aside

```scala
def cached[R, E >: RedisError, A: Schema](key: String, ttl: Duration)(
  compute: ZIO[R, E, A]
): ZIO[R & Redis, E, A] =
  for
    redis <- ZIO.service[Redis]
    hit   <- redis.get(key).returning[A]
    value <- hit match
               case Some(v) => ZIO.succeed(v)
               case None    => compute.tap(v => redis.set(key, v, Some(ttl)))
  yield value
```

Decide deliberately whether a cache failure should fail the request. Usually it should
not — `.catchAll(_ => compute)` around the lookup makes Redis an optimization rather than
a dependency. But then you must also bound `compute`, or a Redis outage turns into a
thundering herd on your database.

### Always set a TTL

A `set` without an expiry is a permanent memory allocation. Unbounded key growth is the
most common way a Redis instance falls over. If a key genuinely has no natural expiry,
that's a sign it belongs in your database instead.

### Distributed locks — read this before using one

`SET key value NX PX ttl` gives you a lock that expires. It does **not** give you mutual
exclusion under all conditions: a process can pause (GC, scheduler) past its TTL and
resume believing it still holds the lock. Redlock is contested for exactly this reason.

Use a Redis lock only where a double-execution is tolerable — deduplicating a scheduled
job, say. For correctness-critical mutual exclusion (money, inventory), use a database
constraint or a version check (`references/persistence.md`). If you do take a lock,
always release it with the token you wrote, inside `ZIO.acquireReleaseWith`, so
interruption can't strand it.

### Rate limiting and idempotency

`incr` plus `expire` on a windowed key is a serviceable rate limiter. `setNx` on an
idempotency key stops duplicate processing of the same Kafka message — but only as a
fast path; the durable guarantee still belongs in the database.

## Never `keys *`

`keys` is O(n) and blocks the single-threaded Redis server for the duration. On a
production instance it is an outage. Use `scan` with a cursor, which is incremental:

```scala
redis.scan(0L, Some("session:*"), Some(100L)).returning[String]
```

## Pub/Sub

Subscriptions come from `RedisSubscription`, a separate service, because a subscribed
connection cannot issue normal commands. Messages arrive as a `ZStream`, so everything in
`references/streams.md` applies — backpressure, `mapZIOPar`, chunking.

Redis pub/sub is fire-and-forget: a subscriber that's down misses messages, with no
replay. It is not a substitute for Kafka. Use it for cache invalidation and presence, not
for anything you must not lose.

## Errors and resilience

`RedisError` is a typed hierarchy — `ProtocolError`, `WrongType`, connection failures.
Handle them at the service boundary rather than `.orDie`-ing them: whether a Redis
failure is fatal to a request is a business decision, and the type forces you to make it.

Wrap calls with `.timeout` and a bounded, jittered retry. A Redis client that retries
forever on a dead instance holds fibers and turns a degraded cache into a down service.

## Testing

```scala
spec.provideShared(
  EmbeddedRedis.layer.orDie,
  RedisExecutor.layer,
  Redis.layer,
  ZLayer.succeed[CodecSupplier](ProtobufCodecSupplier)
) @@ TestAspect.sequential
```

`zio-redis-embedded` supplies the `RedisConfig` from an in-process server. `sequential`
matters — tests sharing one Redis instance will clash on keys otherwise. Prefix keys per
test or flush between them.

For unit-testing business logic, hide Redis behind your own `Cache` trait and provide an
in-memory `Ref`-backed implementation. The embedded server is for verifying the Redis
code itself.

## Checklist

- One `CodecSupplier` per application, chosen deliberately and never changed casually
- Every `set` has a TTL, or a documented reason it doesn't
- No `keys` — `scan` with a cursor
- Cache failures degrade to the source of truth rather than failing the request, with the
  source-of-truth path bounded
- `.returning[A]` types match what was written, including across deploys
- Locks used only where double-execution is tolerable; released via `acquireRelease`
- `.timeout` and bounded jittered retries on every call
- `RedisError` mapped to a domain decision, not `.orDie`
- Credentials via `Config.Secret`; check current TLS support before assuming it
- Pub/Sub not used for anything that must not be lost

# Persistence

Relational database access from ZIO: connection pooling as a layer, transaction
boundaries, and repository design.

## Choosing a library

| Library | Shape | State |
|---|---|---|
| **zio-jdbc** | `sql"..."` interpolator, `ZConnectionPool`, `transaction { }` | Lives under `zio-archive` and has had maintenance gaps. Fine for small services; verify it's still moving before adopting it for something long-lived. |
| **Quill** (`zio-quill`) | Compile-time query generation from case classes | Actively used; queries checked at compile time; steep macro error messages |
| **doobie** | cats-effect based | Mature and well documented, but needs `zio-interop-cats` |
| **zio-sql** | Type-safe relational DSL | Long-running; check current status before committing |
| **zio-blocks-sql** | Newer schema-driven SQL with `TransactorZIO` | Scala 3 / JVM only; young |

There is no consensus winner here. Check what the repo already uses and stay with it —
mixing two database libraries in one service is worse than either choice.

## zio-jdbc

```scala
import zio.jdbc._

val poolConfig: ULayer[ZConnectionPoolConfig] =
  ZLayer.succeed(ZConnectionPoolConfig.default)

val pool: ZLayer[ZConnectionPoolConfig, Throwable, ZConnectionPool] =
  ZConnectionPool.postgres("localhost", 5432, "mydb", Map("user" -> "u", "password" -> "p"))
```

Pre-built pools exist for Postgres, MySQL, SQL Server, Oracle and H2. In real code the
credentials come from config, not literals:

```scala
val poolLayer: ZLayer[Any, Throwable, ZConnectionPool] =
  ZLayer.succeed(ZConnectionPoolConfig.default) >>>
    ZLayer.scoped {
      for
        cfg  <- ZIO.config[PostgresConfig]
        pool <- ZConnectionPool
                  .postgres(cfg.host, cfg.port, cfg.database,
                            Map("user" -> cfg.user,
                                "password" -> String(cfg.password.value.toArray)))
                  .build.map(_.get[ZConnectionPool])
      yield pool
    }
```

Note the `Secret` only becomes a `String` at the call site that needs it — see
`references/config.md`.

### Queries

```scala
val q: Query[User] = sql"select name, age from users where age > $minAge".query[User]

transaction(q.selectAll)       // ZIO[ZConnectionPool, Throwable, Chunk[User]]
transaction(q.selectOne)       // Option[User]
transaction(sql"delete from users where id = $id".delete)
transaction(sql"insert into users (name, age)".values(user).insert)
```

The interpolator parameterizes every `$` — it does not splice. That's the SQL-injection
protection, and it's why you must never build a query by string concatenation. Dynamic
fragments compose with `SqlFragment` values, not with `s"..."`.

Decoders come from `JdbcDecoder.fromSchema` when you already have a `Schema[A]` (see
`references/schema-and-json.md`), or are written by hand for tuples.

## Transactions

`transaction { }` acquires a connection, runs the effect, commits on success and rolls
back on failure **or interruption**. The critical rules:

- **A transaction is a scope, not a value.** Everything that must be atomic has to be
  *inside* the same `transaction` block. Two `transaction` calls are two transactions,
  exactly like two `Ref.update` calls are two atomic operations (`SKILL.md`, concurrency
  section).
- **Never `.fork` inside a transaction.** The forked fiber does not inherit the
  connection in any meaningful way, and the transaction may commit or roll back before
  the child finishes.
- **Never `.retry` a transaction from the inside.** Wrap the whole `transaction { }` in
  the retry policy, so each attempt is a fresh transaction. Retrying an effect that has
  already written inside an open transaction repeats the write.
- Keep transactions short. A transaction held open across an HTTP call to a third party
  is a connection-pool exhaustion incident waiting to happen.
- `.timeout` around a transaction interrupts it and triggers rollback — which is what you
  want, but make sure the timeout is longer than the DB's own statement timeout so you
  get the useful error rather than a generic interruption.

```scala
// Right: retry the whole transaction
transaction(transferFunds(from, to, amount))
  .retry(Schedule.exponential(50.millis).jittered && Schedule.recurs(3))
```

### Idempotency

At-least-once delivery upstream (Kafka) means your writes will be replayed. Push
idempotency into the database rather than into application logic:

- A unique constraint on a natural or supplied idempotency key, with
  `on conflict do nothing` / `do update`
- An `processed_events(event_id)` table written in the *same* transaction as the effect
- Optimistic concurrency: `update ... where id = $id and version = $expected`, then check
  the affected row count and fail with a typed `ConflictError` when it's zero

The version check must be part of the `update` statement, not a separate `select` — a
read-then-write pair is a race no matter how it's wrapped.

## Repository design

Keep the persistence library out of the service interface:

```scala
trait UserRepo:
  def find(id: UserId): IO[RepoError, Option[User]]
  def save(user: User): IO[RepoError, Unit]

final case class UserRepoLive(pool: ZConnectionPool) extends UserRepo:
  def find(id: UserId): IO[RepoError, Option[User]] =
    transaction(sql"select * from users where id = $id".query[User].selectOne)
      .provideEnvironment(ZEnvironment(pool))
      .refineOrDie { case e: SQLException => RepoError.from(e) }
```

- The trait's methods have environment `Any`. `ZConnectionPool` is a constructor
  parameter, per the service pattern.
- Translate `SQLException` into domain errors at this boundary. A unique-violation SQLSTATE
  becoming `RepoError.Conflict` is what lets the caller handle it without knowing about JDBC.
- Don't leak `Query`, `SqlFragment` or `ZConnectionPool` through the interface. The point
  of the trait is that an in-memory implementation can satisfy it.

### Where does the transaction boundary live?

If a use case must write to two repositories atomically, the boundary belongs in the
service layer, not inside each repository method — otherwise each call opens its own
transaction. Two workable shapes:

1. Repository methods return `ZIO[ZConnectionPool, RepoError, A]` and the service wraps
   them in one `transaction { }`. Honest, but the pool leaks into the signature.
2. The service exposes a `Transactor`-style capability (`def atomically[A](zio: IO[E, A]): IO[E, A]`)
   that owns the boundary. Keeps signatures clean at the cost of one more abstraction.

Pick one per codebase and be consistent; mixing them produces silent nested-transaction
bugs.

## Streaming large result sets

Do not `selectAll` a table. Stream it, so memory stays bounded and downstream
backpressure reaches the cursor:

```scala
transaction(sql"select * from events".query[Event].selectStream)
```

Then everything in `references/streams.md` applies — `grouped`, `mapZIOPar`, sinks.

## Testing

- **Testcontainers** is the honest default. A pool layer built from a container, provided
  with `provideShared` so it starts once per suite:
  ```scala
  spec.provideShared(containerLayer >>> poolLayer) @@ TestAspect.sequential
  ```
- Run migrations (Flyway/Liquibase) as part of the layer's acquire step so the schema
  under test is the schema you deploy.
- An in-memory `UserRepo` implementation is right for testing *business logic*; it proves
  nothing about your SQL. Both are needed, at different levels.
- H2 in Postgres-compatibility mode is faster but diverges on exactly the things that
  break in production — upserts, JSON columns, isolation levels.

## Checklist

- Pool built as a scoped layer, closed on shutdown
- Pool size bounded and tied to the service's parallelism, not left at a default
- Credentials via `Config.Secret`, never in the connection string in source
- No string concatenation anywhere near SQL
- Atomic work inside one `transaction`, with no `.fork` and no inner `.retry`
- Retries around the transaction, jittered and bounded
- `SQLException` translated to typed domain errors at the repository boundary
- Unique constraint or version check enforcing idempotency in the database
- Large reads streamed, not collected
- Statement timeout set on the DB side as well as `.timeout` on the effect

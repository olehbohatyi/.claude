# ProtoQuill (Quill for Scala 3)

```scala
"io.getquill" %% "quill-jdbc-zio" % V     // ZIO + JDBC
"org.postgresql" % "postgresql"   % V     // plus your driver
```

Note the group is **`io.getquill`, not `dev.zio`** — a repo using Quill may not declare
any `dev.zio` artifact at all beyond transitive ZIO. Scala 3 artifacts are the
`quill-<module>_3` line; `%%` resolves this for you.

Quill writes SQL *at compile time*. You express queries as Scala over case classes inside
a quotation; the macro parses them, builds an AST, and emits the SQL string during
compilation. Runtime cost is close to using the driver directly, because there is no
query building at runtime.

Turn on the macro log to see what SQL you actually get — this is the single most useful
setting in the library:

```
-Dquill.macro.log=true
```

## The Scala 3 trap: inline or it goes dynamic

**This is the thing to know before anything else.** In ProtoQuill, a quotation stays
compile-time only if it is reachable as an `inline def`/`inline given`. A plain `val` or
`def` holding a quote still compiles and still works — but the query is built at
*runtime* instead. You lose the compile-time SQL, the performance characteristic, and the
compile-time validation, silently.

```scala
inline def adults = quote { query[Person].filter(_.age >= 18) }   // compile-time
def adultsDynamic  = quote { query[Person].filter(_.age >= 18) }   // still works — dynamic
```

Scala 2 Quill code ported over mostly compiles unchanged, and mostly becomes dynamic.
When reviewing a migration, `inline` is the first thing to check. When a query is
unexpectedly slow to build or the macro log shows nothing for it, this is why.

## Schema mapping

Case classes map to tables by name; a `NamingStrategy` bridges Scala naming to SQL
naming.

```scala
final case class Person(id: Long, firstName: String, age: Int)
// with SnakeCase: person(id, first_name, age)
```

Overrides are `inline given` in ProtoQuill:

```scala
inline given SchemaMeta[Person] = schemaMeta("person_table", _.firstName -> "fname")
inline given InsertMeta[Person] = insertMeta(_.id)      // exclude generated id
inline given UpdateMeta[Person] = updateMeta(_.id)
```

Drop the `inline` and the query goes dynamic, per above.

## Queries and actions

```scala
inline def byAge(inline min: Int) = quote {
  query[Person].filter(p => p.age >= lift(min)).sortBy(_.age)
}

inline def insert(inline p: Person) = quote { query[Person].insertValue(lift(p)) }
inline def deleteOld = quote { query[Person].filter(_.age > 100).delete }
```

`lift` marks a runtime value, which becomes a bound `?` parameter — that's the
injection-safe path and the reason a lifted query can be prepared once. `liftQuery`
handles collections (`IN` clauses) and batch actions; ProtoQuill supports batch insert,
update and delete both compile-time and at runtime.

Joins, aggregations, nested queries and `returning` follow the Scala 2 Quill
documentation, which remains the reference for query syntax.

## ZIO wiring

The ZIO context slots neatly into the service pattern — take the context as a
constructor parameter, exactly like any other dependency:

```scala
import io.getquill._, io.getquill.jdbczio.Quill

final case class PersonRepoLive(quill: Quill.Postgres[SnakeCase]) extends PersonRepo:
  import quill._

  def all: ZIO[Any, SQLException, List[Person]] = run(query[Person])

object PersonRepoLive:
  val layer = ZLayer.fromFunction(PersonRepoLive.apply)
```

```scala
program.provide(
  PersonRepoLive.layer,
  Quill.Postgres.fromNamingStrategy(SnakeCase),
  Quill.DataSource.fromPrefix("myDatabaseConfig")
)
```

`Quill.DataSource.fromPrefix` reads a HOCON block and builds a Hikari pool. That's
convenient but it bypasses `ZIO.config`, so those settings live outside your `Config`
descriptors and your secrets are in the file rather than in `Config.Secret`. If that
matters, build the `DataSource` yourself from your own config and provide it as a layer —
see `references/config.md`.

`run` fails with `SQLException`. Translate it to a domain error at the repository
boundary rather than letting `SQLException` leak through the trait
(`references/persistence.md`).

## Transactions

```scala
import quill._
transaction {
  for
    _ <- run(debit)
    _ <- run(credit)
  yield ()
}
```

Everything in `references/persistence.md` applies unchanged: one `transaction` block per
atomic unit, no `.fork` inside, retry the whole block from outside rather than any `run`
within it, and keep the block short.

## Streaming

```scala
stream(query[Event])     // ZStream[Any, Throwable, Event]
```

Use this instead of `run` on any large table — it keeps memory bounded and lets
downstream backpressure reach the cursor. From there, `references/streams.md` applies.

## Extensions

The Scala 2 `implicit class` pattern is **not supported**. Use an `extension` with
`inline def`:

```scala
extension (q: Query[Person])
  inline def olderThan(inline age: Int) =
    quote { q.filter(p => p.age > lift(age)) }
```

For SQL that Quill can't express, `sql"..."` (formerly `infix`) splices raw fragments —
sparingly, and never with interpolated user input that isn't a `lift`.

## Dynamic queries, deliberately

Runtime-composed filters (a search endpoint with optional criteria) genuinely need
dynamic queries. That's fine — just make it a choice rather than an accident, and
comment it, so the next reader doesn't "fix" it by adding `inline` and breaking
compilation. Keep the dynamic surface small and the hot paths inline.

## Compile-time cost

Macro expansion is not free. A module with hundreds of inline queries compiles
noticeably slower, and macro errors are famously opaque — a type error deep in a
quotation often surfaces as a wall of AST. When one appears: simplify the quotation until
it compiles, then add pieces back. Most of them turn out to be an unlifted runtime value
or an unsupported method inside the quote.

## Testing

- Testcontainers plus the real `DataSource` layer, `provideShared` per suite — the SQL is
  the thing under test, and H2 diverges from Postgres exactly where it matters.
- Pure query logic can be checked without a database: with `quill.macro.log` on, the
  generated SQL appears at compile time, and a mismatch shows up as a compile-time diff
  in review rather than a runtime surprise.
- Business logic tests go through your repository trait with an in-memory implementation;
  don't route them through Quill at all.

## Checklist

- Quotations are `inline def` / `inline given` — no accidental dynamic queries
- `quill.macro.log` enabled at least once to confirm the generated SQL
- Every runtime value goes through `lift` / `liftQuery`; no string interpolation into SQL
- `sql"..."` fragments justified and free of unlifted input
- Context taken as a constructor parameter, per the service pattern
- `SQLException` mapped to domain errors at the repository boundary
- Transaction boundaries per `references/persistence.md`; retries outside the block
- Large reads use `stream`, not `run`
- Pool settings and credentials sourced deliberately — `fromPrefix` bypasses `ZIO.config`
- Naming strategy fixed and explicit; schema overrides via `inline given`

# zio-config

Type-safe application configuration. `Config[A]` describes *what* to read;
`ConfigProvider` describes *where* to read it from. ZIO core ships both; the
`zio-config-*` modules add file-based providers and derivation.

```scala
"dev.zio" %% "zio-config"           % V   // derivation, helpers
"dev.zio" %% "zio-config-typesafe"  % V   // HOCON
"dev.zio" %% "zio-config-yaml"      % V   // YAML
"dev.zio" %% "zio-config-magnolia"  % V   // automatic derivation
```

Check the version in the build — do not guess one.

## Describing configuration

Primitives: `Config.string`, `Config.int`, `Config.long`, `Config.double`,
`Config.boolean`, `Config.duration`, `Config.uri`, `Config.secret`.

```scala
Config.string("host")            // read key "host"
Config.string.nested("host")     // identical, explicit form
Config.int("port").nested("server")   // server.port
```

Collections: `Config.listOf`, `Config.chunkOf`, `Config.setOf`, `Config.vectorOf`,
`Config.table` (string-keyed map).

Modifiers:

```scala
Config.int("port").withDefault(8080)
Config.boolean("cors").optional                       // Config[Option[Boolean]]
Config.int("port").validate("must be 1..65535")(p => p >= 1 && p <= 65535)
Config.string.mapOrFail(parseUuid)                    // fallible transform
```

Validate at load time. A malformed value should fail the deploy at startup, not the
first request that touches it.

## Product and sum types

`zip` builds products, `orElse` builds sums, `nested` adds a path segment.

```scala
import zio.Config.Secret

final case class PostgresConfig(host: String, port: Int, user: String, password: Secret)

object PostgresConfig:
  given Config[PostgresConfig] =
    (Config.string("host") zip
     Config.int("port").withDefault(5432) zip
     Config.string("user") zip
     Config.secret("password"))
      .map(PostgresConfig.apply)
      .nested("postgres")
```

```scala
sealed trait StorageConfig
final case class S3(bucket: String)  extends StorageConfig
final case class Local(path: String) extends StorageConfig

given Config[StorageConfig] = S3.config orElse Local.config
```

`orElse` is a *fallback*, not exclusive choice: it tries the first and falls back on
failure, so a source containing both is accepted. If mutual exclusivity matters, add a
`validate` that rejects the ambiguous case.

With `zio-config-magnolia`, `deriveConfig[MyConfig]` generates the descriptor from the
case class. Convenient, but it couples your field names to your config keys — write the
descriptor by hand when the external format is fixed or must stay stable.

## Providers

```scala
ConfigProvider.envProvider          // environment variables
ConfigProvider.propsProvider        // system properties
ConfigProvider.fromMap(map)         // tests
ConfigProvider.fromAppArgs          // command line
ConfigProvider.fromHoconFile(file)  // zio-config-typesafe
ConfigProvider.fromYamlFile(file)   // zio-config-yaml
```

Default in ZIO is `envProvider orElse propsProvider`. Override it for the whole
application in `bootstrap`, where it applies before anything else runs:

```scala
object Main extends ZIOAppDefault:
  override val bootstrap: ZLayer[Any, Any, Any] =
    Runtime.setConfigProvider(
      ConfigProvider.fromHoconFile(new File("application.conf"))
        .orElse(ConfigProvider.envProvider)      // env wins nothing; first wins
    )

  def run = program
```

Order matters: the *first* provider that succeeds wins. Put the file first and env second
if the file is the source of truth; reverse it if env vars should override the file
(the usual container setup).

### Nested vs flat providers

HOCON, YAML, JSON and XML providers are **nested** — they understand hierarchy natively.
Env vars, properties and args are **flat**: nesting and lists are encoded with
delimiters.

```scala
ConfigProvider.fromEnv(pathDelim = "_", seqDelim = ",")
```

```
POSTGRES_HOST=db.internal
POSTGRES_PORT=5432
POSTGRES_PASSWORD=...
DATABASE_ALLOWED_SCHEMAS=public,auth,audit
```

Mind the delimiter when a key naturally contains the path separator — `max-size` under
`_` nesting is unambiguous, but `max_size` is not.

## Consuming config

```scala
val program: ZIO[Any, Config.Error, Unit] =
  ZIO.config[PostgresConfig].flatMap { cfg => ... }
```

Better: read it in the layer, so the service receives plain values and never touches the
config system:

```scala
object PostgresRepoLive:
  val layer: ZLayer[Any, Config.Error, UserRepo] =
    ZLayer {
      for
        cfg  <- ZIO.config[PostgresConfig]
        pool <- makePool(cfg)
      yield PostgresRepoLive(pool)
    }
```

This keeps `Config.Error` at startup where it belongs, and makes the service trivially
testable with a hand-built instance.

## Secrets

Use `Config.secret` for every credential, token, and key. `Config.Secret`:

- keeps the value out of `toString` — it can't leak through a logged case class
- compares in constant time, closing a timing side channel on token equality
- is `final`, so it can't be subclassed into something that leaks
- exposes `.value` (a `Chunk[Char]`) only where you explicitly ask, and `.wipe` to zero
  the memory when done

Never widen a `Secret` into a `String` field to make some API happy — convert at the call
site, as late as possible.

In HOCON, keep the secret out of the file and let it come from the environment:

```hocon
postgres {
  host = "localhost"
  password = ${?POSTGRES_PASSWORD}
}
```

## Testing

```scala
val testConfig = ConfigProvider.fromMap(
  Map("postgres.host" -> "localhost", "postgres.port" -> "5432", ...)
)

test("...") { ... } @@ TestAspect.withConfigProvider(testConfig)
```

`fromMap` takes a `pathDelim` too if your keys are nested. For a whole suite, set the
provider once on the suite rather than per test.

## Review checklist

- Credentials typed as `String` instead of `Config.Secret`
- Config read deep inside business logic rather than at layer construction
- No `validate` on values with real constraints (ports, pool sizes, timeouts)
- `withDefault` hiding a value that should be mandatory in production
- Secrets committed in `application.conf` instead of interpolated from the environment
- Provider order that makes env vars *unable* to override the packaged file

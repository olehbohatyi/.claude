# ZIO CLI

```scala
"dev.zio" %% "zio-cli" % V
```

Declarative command-line applications. You describe commands, options and arguments as
values; zio-cli parses `argv`, generates `--help`, validates input, and hands you a typed
model.

## Shape of an app

```scala
import zio._, zio.cli._
import zio.cli.HelpDoc.Span.text

object Migrate extends ZIOCliDefault:

  val verbose: Options[Boolean] = Options.boolean("verbose").alias("v")
  val target:  Args[String]     = Args.text("target")

  val command: Command[(Boolean, String)] =
    Command("migrate", verbose, target)
      .withHelp(HelpDoc.p("Run database migrations up to a target version"))

  val cliApp = CliApp.make(
    name    = "migrate",
    version = "1.0.0",
    summary = text("Database migration tool"),
    command = command
  ) { case (verbose, target) =>
    runMigration(target, verbose).provide(AppLayers.live)
  }
```

`ZIOCliDefault` supplies `run` from your `cliApp` — you don't override it. Layers are
provided inside the handler, where you know which commands need what.

The order in `Command(name, options, args)` is fixed: name, then options, then args. The
resulting `Command[Model]` is typed by what the parse produces.

## Options, Args, and the difference

**Options** are named and position-independent (`--verbose`, `-v`). **Args** are
positional (`migrate v3`). Help output shows args in `<angle brackets>` and options
prefixed with `--`.

```scala
Options.boolean("force").alias("f")        // presence = true, no value needed
Options.text("name")
Options.integer("retries").withDefault(BigInt(3))
Options.file("config", Exists.Yes)          // validated to exist
Options.directory("out")
Options.enumeration("level")("debug" -> Debug, "info" -> Info)
Options.text("token").optional              // Options[Option[String]]
```

```scala
Args.text("repository")
Args.file("input", Exists.Yes)
Args.integer("count")                       // BigInt — arbitrary precision
Args.instant("since")                       // ISO-8601, validated
Args.text("files").repeat                   // Args[List[String]]
```

Numeric args produce `BigInt`/`BigDecimal`, and the parser verifies the input really is a
number. File and directory args can assert existence at parse time — do this, so a typo
fails before your effect starts rather than in the middle of the work.

Combining: `++` builds a tuple, `orElse` offers alternatives, `.map` transforms into a
domain type. Validation belongs here, not in your handler:

```scala
Options.integer("port").map(_.toInt).withDefault(8080)
```

## Subcommands and a model type

For anything beyond one command, map each subcommand to a case of a sealed trait and
pattern-match in the handler. This keeps the parse layer and the logic layer separate:

```scala
sealed trait Cmd
object Cmd:
  final case class Up(steps: BigInt)   extends Cmd
  final case class Down(steps: BigInt) extends Cmd
  case object Status                   extends Cmd

val up     = Command("up",     Args.integer("steps")).map(Cmd.Up.apply)
val down   = Command("down",   Args.integer("steps")).map(Cmd.Down.apply)
val status = Command("status").map(_ => Cmd.Status)

val command = Command("migrate").subcommands(up, down, status)

val cliApp = CliApp.make(...)(
  {
    case Cmd.Up(n)   => migrateUp(n)
    case Cmd.Down(n) => migrateDown(n)
    case Cmd.Status  => showStatus
  }
)
```

Exhaustivity checking on the sealed trait means adding a subcommand without handling it
is a compile error — the same reason to use sealed traits for domain errors
(`SKILL.md`).

Shared options can live on the parent command and be combined into each subcommand's
model, rather than repeated on every leaf.

## What you get for free

- `--help` at every level, generated from `HelpDoc`
- `--version`
- Shell completions (bash/zsh) and a built-in wizard mode that walks a user through
  options interactively
- Typed parse errors with usage output, rather than an `ArrayIndexOutOfBoundsException`

`HelpDoc` is a document model, not a string: `HelpDoc.p`, `.h1`, `enumeration`, `blocks`.
Attach it with `.withHelp` on commands and options. Help text written here is the help
text users see; there's nowhere else to put it.

## Known sharp edge

zio-cli has historically **ignored unrecognized trailing arguments** rather than failing
— an open issue in zio-cli. `myapp xyz abc` where only one arg is declared parses as `xyz`
and drops `abc` silently. If mistyped input must not be silently discarded, validate the
remainder yourself. Check whether the version you're on still behaves this way.

## Testing

This is the real payoff over hand-rolled `args(0)` parsing: a `CliApp` is a value, and
`cliApp.run(List("migrate", "up", "3"))` is an ordinary effect.

```scala
test("up parses the step count") {
  for
    exit <- cliApp.run(List("up", "3")).exit
  yield assertTrue(exit.isSuccess)
}
```

Better still, test the `Command` parse separately from the handler logic — the handler is
just a function returning a `ZIO`, so extract it and test it directly with no CLI
involved. The CLI layer should contain no business logic worth testing through `argv`.

## Packaging

A CLI that requires `sbt run` isn't a CLI. Produce a real executable:

- `sbt-native-packager` (`JavaAppPackaging`) for a launcher script
- `scala-cli package --native` or GraalVM `native-image` for a fast-starting single
  binary — worth it, since JVM startup dominates the runtime of a short command
- Keep the JVM's default `ZIOAppDefault` exit-code behavior: failures must exit non-zero,
  or nothing that invokes your tool in a shell pipeline can detect the failure

## Checklist

- Validation expressed in `Options`/`Args` (existence, ranges, enumerations), not in the
  handler
- Subcommands mapped to a sealed trait, handled exhaustively
- `withHelp` on every command and non-obvious option
- Layers provided inside the handler, per subcommand where they differ
- Unrecognized-argument behavior verified for your version
- Secrets never taken as a plain option — a `--password` value lands in the user's shell
  history and in `ps` output. Read from env or a file instead
- Failures exit non-zero
- Packaged as a binary or launcher, not `sbt run`
- Handler logic extracted and tested without going through `argv`

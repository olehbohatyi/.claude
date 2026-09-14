# Testing (zio-test)

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-test"          % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt"      % zioVersion % Test,
  "dev.zio" %% "zio-test-magnolia" % zioVersion % Test   // derived Gen instances
)
testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
```

## Structure

```scala
import zio._, zio.test._, zio.test.Assertion._

object MySpec extends ZIOSpecDefault {
  def spec = suite("MySpec")(
    test("pure")(assertTrue(1 + 1 == 2)),
    test("effectful") {
      for { v <- MyService.compute } yield assertTrue(v == 42)
    },
    suite("nested")( /* ... */ )
  ) @@ TestAspect.timeout(30.seconds)
}
```

A `Spec` is a recursive tree of suites and tests. A test **is** an effect returning `TestResult` — never call `unsafeRun` inside one.

`ZIOSpecDefault` provides the `TestEnvironment`. Use `ZIOSpec[R]` with a custom `bootstrap` layer when every test in the file needs the same extra services.

## Assertions

Prefer **smart assertions**:

```scala
assertTrue(user.name == "ada")
assertTrue(list.length == 5 && list.forall(_ > 0))
assertTrue(result.is(_.right).id == expectedId)     // test lens
assertTrue(exit.is(_.die).getMessage == "boom")
```

`assertTrue` is a macro: it decomposes the boolean expression and reports exactly which sub-expression failed and with what values. Combine with `&&` / `||` inside a single `assertTrue` to keep the diff informative.

Test lenses available via `.is(_.…)`: `some`, `left`, `right`, `success`, `failure` (Try), `die`, `interrupted`, `subtype[T]`, `custom(CustomAssertion.make[A] { ... })`.

Classic assertions are still there when you need a named, reusable predicate:

```scala
assert(value)(equalTo(42) && isGreaterThan(0))
assertZIO(effect)(isSome(hasField("name", _.name, equalTo("ada"))))
assert(list)(hasSize(equalTo(3)) && contains(2) && forall(isPositive))
assertZIO(effect.exit)(fails(equalTo(MyError)))
assertZIO(effect.exit)(dies(isSubtype[IllegalArgumentException](anything)))
assertCompletes / assertNever
```

Asserting on failures — two idioms:

```scala
effect.flip.map(e => assertTrue(e == UserNotFound(id)))          // typed error
assertZIO(effect.exit)(failsWithA[UserNotFound])
```

## Test environment

`TestClock`, `TestRandom`, `TestConsole`, `TestSystem` replace the live services and make tests deterministic.

```scala
test("retries with backoff") {
  for {
    fiber <- flakyEffect.retry(Schedule.exponential(1.second)).fork
    _     <- TestClock.adjust(10.seconds)          // time only moves when you say so
    r     <- fiber.join
  } yield assertTrue(r == expected)
}
```

**The most common test hang:** an effect that sleeps, not forked before `TestClock.adjust`. Fork first, adjust, then join.

```scala
TestClock.adjust(d) / setTime(instant) / setTimeZone(zone) / sleeps
TestRandom.feedInts(1, 2, 3) / setSeed(42) / feedUUIDs(...)
TestConsole.feedLines("input") / output / clearOutput
TestSystem.putEnv("KEY", "v") / putProperty(...)
```

Need the real thing for one test: `@@ TestAspect.withLiveClock`, `@@ withLiveRandom`, or `ZIO.withClock(Clock.ClockLive)(effect)` / `live(effect)`.

## Test aspects

```scala
test(...) @@ TestAspect.timeout(5.seconds)
          @@ TestAspect.nonFlaky           // run 100 times, all must pass
          @@ TestAspect.flaky              // retry until it passes (mark tech debt!)
          @@ TestAspect.repeats(50)
          @@ TestAspect.retries(3)
          @@ TestAspect.sequential         // no parallel execution within suite
          @@ TestAspect.parallelN(4)
          @@ TestAspect.ignore / .failing / .diagnose(10.seconds)
          @@ TestAspect.jvmOnly / .jsOnly / .scala2Only
          @@ TestAspect.before(setup) / .after(cleanup) / .around(a)(r)
          @@ TestAspect.tag("integration")
          @@ TestAspect.withLiveClock
          @@ TestAspect.samples(200)       // property-test sample count
          @@ TestAspect.shrinks(0)         // disable shrinking
          @@ TestAspect.silentLogging
```

Aspects applied to a suite apply to every test in it. Order matters: `@@ a @@ b` applies `a` innermost.

Custom aspect:

```scala
val withTestDb: TestAspect[Nothing, Database, Nothing, Any] =
  TestAspect.around(createSchema)(dropSchema)
```

## Resources in tests

```scala
).provide(ServiceLive.layer, RepoStub.layer)        // new instance per test
).provideShared(PostgresContainer.layer)            // one instance for the whole suite
).provideSomeShared[TestEnvironment](dbLayer)       // keep TestClock etc.
).provideLayer(layer) @@ TestAspect.sequential      // when the shared resource isn't concurrent-safe
```

`provideShared` is how you avoid spinning up a Testcontainer per test. Pair it with `@@ sequential` unless the resource genuinely supports concurrent access.

## Property-based testing

```scala
test("reverse is an involution") {
  check(Gen.listOf(Gen.int)) { xs =>
    assertTrue(xs.reverse.reverse == xs)
  }
}

test("effectful property") {
  checkN(50)(Gen.alphaNumericString, Gen.int(1, 100)) { (name, age) =>
    for { u <- UserService.create(name, age) } yield assertTrue(u.name == name)
  }
}
```

Generators:

```scala
Gen.int / int(min, max) / long / double / boolean / char / alphaNumericString
Gen.const(v) / elements(a, b, c) / fromIterable(xs)
Gen.option(g) / either(g1, g2) / listOf(g) / listOfN(5)(g) / setOf / mapOf
Gen.chunkOf(g) / vectorOf(g)
Gen.oneOf(g1, g2) / weighted(g1 -> 0.8, g2 -> 0.2)
Gen.suspend(g)                     // recursive generators
Gen.fromZIO(effect)
Gen.instant / localDateTime / uuid / finiteDuration
g.map(f) / flatMap / filter / zip / noShrink
```

Derive from a case class with magnolia:

```scala
import zio.test.magnolia._
val userGen: Gen[Any, User] = DeriveGen[User]
```

Generators are `ZStream`s of `Sample`s; each `Sample` carries a tree of smaller values, which is how **shrinking** produces a minimal failing case for free. `Gen.int.noShrink` disables it; `@@ TestAspect.shrinks(0)` disables globally.

`Gen.const` and `Gen.fromIterable` are deterministic; the random ones respect `TestRandom`'s seed, so a failing property is reproducible.

## Mocking

```scala
libraryDependencies += "dev.zio" %% "zio-mock" % zioMockVersion % Test
```

```scala
object MockUserRepo extends Mock[UserRepo] {
  object Find extends Effect[UserId, RepoError, Option[User]]
  val compose: URLayer[Proxy, UserRepo] = ???   // generated by @mockable or written manually
}

val env = MockUserRepo.Find(Assertion.equalTo(id), Expectation.value(Some(user)))
myTest.provide(env.toLayer, ServiceLive.layer)
```

Mocks verify *interaction* (was it called, with what, how many times). For most tests a hand-written in-memory stub backed by a `Ref` is simpler and less brittle — reach for `zio-mock` only when the expectation itself is the thing under test.

```scala
final case class UserRepoInMemory(ref: Ref[Map[UserId, User]]) extends UserRepo {
  def find(id: UserId) = ref.get.map(_.get(id))
  def save(u: User)    = ref.update(_ + (u.id -> u))
}
object UserRepoInMemory {
  val layer = ZLayer(Ref.make(Map.empty[UserId, User]).map(UserRepoInMemory(_)))
}
```

## Test annotations & reporting

```scala
test(...) @@ TestAspect.tag("slow", "db")
sbt> testOnly *UserSpec -- -tags integration
sbt> testOnly *UserSpec -- -ignore-tags slow
```

Built-in annotations record timing, retries, repeats, ignored counts and fibers. `TestAspect.timed` adds duration to the report. Custom annotations via `TestAnnotation.apply[V](identifier, initial, combine)` plus `ZTestLogger`/`Annotations.annotate`.

## Checklist

- No `unsafeRun`, no `Thread.sleep`, no real clock unless `@@ withLiveClock`.
- Test the error channel as deliberately as the success channel.
- One behaviour per test; use `suite` for grouping, not giant multi-assert tests.
- Expensive resources via `provideShared`, cheap stubs via `provide`.
- `@@ nonFlaky` on anything concurrent — it catches race conditions that pass once.
- If a test needs `@@ flaky` to be green, it's a bug report, not a fix.

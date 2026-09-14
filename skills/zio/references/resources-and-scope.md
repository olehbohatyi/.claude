# Resources & Scope

## Why not try/finally

`try/finally` binds cleanup to a *stack frame*. Asynchronous and concurrent code leaves the frame long before the resource is done, and `finally` never runs when a fiber is interrupted. ZIO's resource operators bind cleanup to an *effect's lifetime* instead, and guarantee it runs on success, failure and interruption.

## acquireRelease

```scala
ZIO.acquireReleaseWith(
  acquire = ZIO.attempt(new FileInputStream(path))
)(
  release = is => ZIO.succeed(is.close())      // must not fail: use orDie/ignore
)(
  use = is => readAll(is)
)
```

Guarantees:
- `acquire` runs uninterruptibly — you can't leak a half-acquired resource.
- `release` runs exactly once, uninterruptibly, whatever happens to `use`.
- `use` is interruptible.

Variants: `acquireReleaseExitWith` (release sees the `Exit`), `acquireReleaseInterruptible`.

`effect.ensuring(finalizer)` adds a finalizer without an acquisition step. `effect.onExit`, `onError`, `onInterrupt` are narrower forms.

## Scope

`Scope` reifies "a place finalizers can be registered". It turns resource management into a value you can pass around and compose.

```scala
val file: ZIO[Scope, Throwable, FileInputStream] =
  ZIO.acquireRelease(ZIO.attempt(new FileInputStream(p)))(is => ZIO.succeed(is.close()))

val program: ZIO[Any, Throwable, String] =
  ZIO.scoped {
    for {
      a <- file           // both acquired here
      b <- otherResource
      r <- use(a, b)
    } yield r
  }                       // both released here, in reverse order
```

A `Scope` in the environment type is a compile-time signal: *this workflow still owns resources*. `ZIO.scoped` discharges it.

Key operators:

```scala
ZIO.scoped { ... }                    // open a scope, close on completion
ZIO.scopedWith(scope => ...)
ZIO.addFinalizer(cleanup)             // register in the current scope
ZIO.addFinalizerExit(exit => cleanup)
Scope.make                            // create a scope manually
scope.extend(zio)                     // run a scoped effect in THIS scope
scope.close(exit)
```

### Child scopes

Sometimes an inner resource should outlive an inner block but not the outer one, or vice versa. Create a child scope explicitly:

```scala
ZIO.scoped {
  for {
    child <- Scope.make
    conn  <- child.extend(openConnection)   // lives as long as `child`
    _     <- useConnection(conn)
    _     <- child.close(Exit.unit)         // released here, before the outer scope ends
    _     <- moreWork
  } yield ()
}
```

`ZIO.acquireRelease(Scope.make)(_.close(Exit.unit))` is the composable form.

### Converting scoped effects

```scala
scopedEffect.forkScoped            // fiber lives as long as the scope
ZStream.scoped(scopedEffect)       // one-element stream, resource tied to the stream
ZLayer.scoped { scopedEffect }     // resource tied to the layer's lifetime
scopedEffect.withEarlyRelease      // -> (UIO[Unit], A): release before scope ends
```

## Common patterns

**Connection pool as a layer** — the resource lives exactly as long as the application:

```scala
val layer: ZLayer[Config, Throwable, Pool] =
  ZLayer.scoped {
    ZIO.acquireRelease(createPool)(_.shutdown.orDie)
  }
```

**Per-request resource** — scope inside the handler, not in a layer:

```scala
handler { (req: Request) =>
  ZIO.scoped {
    borrowConnection.flatMap(runQuery)
  }
}
```

**Interleaved acquisition** — `ZIO.foreachPar` over scoped effects acquires in parallel and releases in parallel when the surrounding scope closes.

## Rules

- A `release` must not fail. Use `.orDie` or `.ignoreLogged`; a failing finalizer becomes a defect that masks the real error.
- Don't put slow work in `acquire` — it's uninterruptible, so you can't cancel it.
- Never return a raw resource from a `ZIO.scoped` block. The value escapes but the resource is already closed. If the type checker allows it (`ZIO.scoped(openFile)`), that's a bug — return the *result* of using it.
- Prefer `ZLayer.scoped` over manual scope plumbing for anything application-lifetime.
- `ZManaged` is ZIO 1.x. In 2.x it's `ZIO[Scope, E, A]`. If you see `ZManaged`, that's a migration signal.

# ZIO Streams

```scala
libraryDependencies += "dev.zio" %% "zio-streams" % zioVersion   // same version as zio core
import zio.stream._
```

## The four types

| Type | Meaning |
|---|---|
| `ZStream[R, E, A]` | produces **zero or more** `A`, incrementally |
| `ZSink[R, E, In, L, Z]` | consumes `In`, produces a summary `Z`, may leave leftovers `L` |
| `ZPipeline[Env, Err, In, Out]` | stream transformation, `ZStream[In] => ZStream[Out]` |
| `ZChannel[Env, InErr, InElem, InDone, OutErr, OutElem, OutDone]` | the primitive all three are built on |

`ZStream` is a `ZChannel` with no input; `ZSink` is one with no meaningful output elements; `ZPipeline` is one with both. You rarely write `ZChannel` directly — reach for it only for stateful transformations the built-in operators can't express.

Streams are **chunked** internally (`Chunk[A]`) for throughput. Most operators have `*Chunks` variants when you need to control chunking.

## Construction

```scala
ZStream(1, 2, 3)
ZStream.fromIterable(list)
ZStream.fromIterator(it)
ZStream.fromZIO(effect)                     // single element
ZStream.fromZIOOption(effect)               // fails with None to end the stream
ZStream.scoped(scopedEffect)                // resource tied to the stream
ZStream.repeat(value) / repeatZIO(effect) / repeatZIOChunk(effect)
ZStream.repeatZIOWithSchedule(effect, Schedule.spaced(1.second))
ZStream.unfold(s)(f) / unfoldZIO / unfoldChunkZIO    // stateful generation
ZStream.iterate(0)(_ + 1)
ZStream.tick(1.second)
ZStream.range(0, 100)
ZStream.fromQueue(q) / fromHub(hub) / fromChunkQueue
ZStream.async[R, E, A](cb => ...)           // callback-driven sources
ZStream.fromFile(path) / fromInputStream / fromResource
ZStream.paginateZIO(cursor)(f)              // paged APIs
ZStream.acquireReleaseWith(acquire)(release)
ZStream.never / empty / fail(e) / die(t)
```

`unfoldZIO` is the workhorse for "read from a cursor until exhausted". `paginateChunkZIO` is the cleanest way to stream a paginated HTTP API.

## Running

```scala
stream.runCollect              // ZIO[R, E, Chunk[A]]  — only for bounded streams!
stream.runDrain                // run for effects
stream.runHead / runLast / runCount / runSum
stream.runFold(z)(f) / runFoldZIO
stream.run(sink)               // general form
stream.foreach(f)              // == run(ZSink.foreach(f))
stream.runForeachChunk(f)
stream.toIterator / toQueue / toInputStream   // scoped
```

`runCollect` on an infinite or large stream is the classic memory bug. Use `take(n)`, a `ZSink`, or `runForeach`.

## Transformation

```scala
stream.map(f) / mapZIO(f) / mapZIOPar(n)(f) / mapZIOParUnordered(n)(f)
stream.mapChunks(f) / mapChunksZIO(f)
stream.mapAccum(s)(f) / mapAccumZIO                 // stateful map
stream.filter / filterZIO / filterNot
stream.collect { case x if p(x) => g(x) }           // filter + map
stream.collectWhile / collectUntil / collectZIO
stream.take(n) / takeWhile / takeUntil / takeRight
stream.drop(n) / dropWhile / dropUntil
stream.flatMap(a => ZStream(...))                   // sequential
stream.flatMapPar(n)(f)                             // concurrent inner streams
stream.flattenChunks / flattenIterables / flattenExitOption
stream.scan(z)(f) / scanZIO                         // emits intermediate states
stream.changes / changesWith(f)                     // dedupe consecutive
stream.zipWithIndex / zipWithNext / zipWithPrevious
stream.tap(f) / tapError / tapSink
stream.via(pipeline)
stream.transduce(sink)                              // repeatedly apply a sink
stream.aggregateAsync(sink)                         // async boundary + aggregation
stream.aggregateAsyncWithin(sink, schedule)         // time/size-based batching
```

**`mapZIOPar(n)` preserves order; `mapZIOParUnordered(n)` doesn't but is faster.** Choose deliberately.

### Grouping and batching

```scala
stream.grouped(100)                                 // Chunk of 100
stream.groupedWithin(100, 5.seconds)                // 100 OR 5 seconds — the batching primitive
stream.groupByKey(f) { (k, s) => ... }              // scoped sub-streams per key
stream.groupBy(f)(...)
stream.partition(p) / partitionEither
stream.split(p) / splitOnChunk(delim)
```

`groupedWithin(n, d)` is what you want for batched writes to a database or an HTTP bulk endpoint.

### Flow control

```scala
stream.buffer(capacity)                // decouple producer and consumer
stream.bufferDropping(n) / bufferSliding(n) / bufferUnbounded
stream.throttleShape(units, duration)(costFn)   // back-pressure to a rate
stream.throttleEnforce(units, duration)(costFn)  // drop excess
stream.debounce(1.second)
stream.schedule(Schedule.spaced(100.millis))
stream.timeout(30.seconds) / timeoutFail(e)(d)
stream.interruptWhen(promise) / haltWhen(effect)
stream.retry(schedule)
stream.rechunk(n)
```

Without a `buffer`, producer and consumer run in lockstep — the natural back-pressure. Add a buffer only where you measured a benefit.

### Broadcasting and distributing

```scala
ZIO.scoped {
  stream.broadcast(2, maximumLag = 16).flatMap { case Chunk(s1, s2) =>
    s1.runDrain zipPar s2.runCount
  }
}
stream.distributedWith(n, maxLag, decide)   // partition elements across n streams
stream.toHub(capacity)
stream.broadcastDynamic(maxLag)
```

`broadcast` sends every element to every consumer; `distributedWith` sends each element to one. Both are scoped.

### Combining streams

```scala
s1 ++ s2                       // concatenation
s1 merge s2                    // interleave as available
ZStream.mergeAll(n)(s1, s2, s3)
s1.mergeHaltLeft(s2) / mergeHaltRight / mergeHaltEither
s1 zip s2                      // pairwise, stops at shorter
s1.zipWith(s2)(f)
s1.zipLatest(s2) / zipWithLatest(s2)(f)    // combine most recent values
s1.zipAll(s2)(d1, d2)
s1 cross s2                    // cartesian product
s1 orElse s2                   // fallback on failure
s1.interleave(s2) / interleaveWith(s2)(boolStream)
s1.combine(s2)(state)(f)       // fully custom stateful combination
```

## Sinks

```scala
ZSink.collectAll[A]
ZSink.count / sum / head / last
ZSink.foreach(f) / foreachChunk(f)
ZSink.fold(z)(contPred)(f) / foldLeft(z)(f) / foldZIO
ZSink.take(n)
ZSink.drain
ZSink.fromFile(path) / fromOutputStream / fromQueue / fromHub / fromPush
ZSink.mkString
ZSink.succeed(z) / fail(e)
```

Composition:

```scala
sink1 zipPar sink2             // feed both, get both results
sink1 <&> sink2
sink1 zip sink2                // run sink1, then sink2 on the leftovers
sink1 race sink2
sink.map(f) / mapZIO / contramap(f) / contramapChunks
sink.collectAllWhile(p)
sink.untilOutput(p)
```

The leftover type parameter `L` is what makes `zip` work: a sink that consumes 5 elements hands the rest to the next sink.

## Pipelines

Reusable, composable stream transformations:

```scala
ZPipeline.map(f) / mapZIO(f)
ZPipeline.filter(p)
ZPipeline.take(n) / drop(n)
ZPipeline.utf8Decode / utf8Encode
ZPipeline.splitLines / splitOn(",")
ZPipeline.grouped(n) / groupedWithin(n, d)
ZPipeline.gzip / gunzip / deflate / inflate
ZPipeline.fromFunction(f) / fromChannel(channel)
ZPipeline.identity

val parseCsv = ZPipeline.utf8Decode >>> ZPipeline.splitLines >>> ZPipeline.map(parseRow)
stream.via(parseCsv)
```

Build domain pipelines as named values — they're testable in isolation and composable with `>>>`.

## ZChannel — when you need it

Reach for `ZChannel` when a transformation needs state that spans chunks and isn't expressible with `mapAccum`/`aggregate`:

```scala
def dedupe[A]: ZPipeline[Any, Nothing, A, A] = {
  def loop(seen: Set[A]): ZChannel[Any, Nothing, Chunk[A], Any, Nothing, Chunk[A], Any] =
    ZChannel.readWithCause(
      in    = (chunk: Chunk[A]) => {
        val (out, next) = chunk.foldLeft((Chunk.empty[A], seen)) {
          case ((acc, s), a) => if (s(a)) (acc, s) else (acc :+ a, s + a)
        }
        ZChannel.write(out) *> loop(next)
      },
      halt  = ZChannel.refailCause(_),
      done  = _ => ZChannel.unit
    )
  ZPipeline.fromChannel(loop(Set.empty))
}
```

`readWithCause` (rather than `readWith`) preserves the full failure cause — prefer it.

## Error handling

```scala
stream.catchAll(e => ZStream.fromZIO(log(e)) *> fallbackStream)
stream.catchAllCause(c => ...)
stream.orElse(other)
stream.either                     // ZStream[R, Nothing, Either[E, A]]
stream.mapError(f)
stream.retry(Schedule.exponential(1.second))
stream.onError(cause => cleanup)
```

A failure terminates the stream. To keep going past a bad element, make the element itself an `Either` (`mapZIO(f(_).either)`) and filter downstream — this is the standard "dead-letter" pattern.

## Gotchas

- `runCollect` on unbounded streams — the #1 bug.
- `mapZIOPar` without a bound on a stream from an unbounded source.
- Forgetting `ZIO.scoped` around `broadcast`, `groupByKey`, `toQueue` — they return scoped effects.
- `ZStream.fromIterator` on a mutable iterator shared between fibers.
- `groupByKey` keeps a sub-stream open per key; on high-cardinality keys this leaks. Use `groupByKey(f, buffer)` and consume every sub-stream, or restructure.
- Chunking surprises: `stream.take(1)` may still have pulled a whole chunk from the source. Use `rechunk(1)` if the source has expensive per-element side effects.
- `aggregateAsyncWithin` needs the surrounding `Clock`; in tests, advance `TestClock`.

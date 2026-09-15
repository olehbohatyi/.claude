# ZIO Kafka

```scala
libraryDependencies += "dev.zio" %% "zio-kafka" % zioKafkaVersion
import zio._, zio.kafka.consumer._, zio.kafka.producer._, zio.kafka.serde._
```

Check the current version at `https://index.scala-lang.org/zio/zio-kafka`. zio-kafka 3.x targets ZIO 2.1+; the 2.x line is older. Partitions map to `ZStream`s, so everything in `references/streams.md` applies.

## Consuming

```scala
val settings = ConsumerSettings(List("localhost:9092"))
  .withGroupId("my-group")
  .withClientId("my-client")
  .withOffsetRetrieval(Consumer.OffsetRetrieval.Auto(Consumer.AutoOffsetStrategy.Earliest))
  .withPollTimeout(100.millis)
  .withProperty("security.protocol", "SASL_SSL")

val consumer: ZLayer[Any, Throwable, Consumer] = ZLayer.scoped(Consumer.make(settings))

val stream = ZIO.serviceWithZIO[Consumer] { consumer =>
  consumer
    .plainStream(Subscription.topics("orders"), Serde.string, Serde.string)
    .mapZIO(record => handle(record.value).as(record.offset))
    .aggregateAsync(Consumer.offsetBatches)
    .mapZIO(_.commit)
    .runDrain
}
```

**zio-kafka 3 removed the companion accessors.** `Consumer.plainStream(...)` compiled on
2.x; on 3.x you take the service first — `ZIO.serviceWithZIO[Consumer](_.plainStream(...))`
— or hold a `Consumer` as a constructor parameter, per the service pattern. Check the
version in the build before copying either shape. `Consumer.offsetBatches` is a plain
value, not an accessor, and is unchanged.

This is the canonical shape: process, collect offsets, batch-commit. `aggregateAsync(Consumer.offsetBatches)` merges offsets so you commit once per batch instead of per record.

Add time-based commits so a slow topic still commits:

```scala
.aggregateAsyncWithin(Consumer.offsetBatches, Schedule.fixed(5.seconds))
```

### Per-partition streams

`plainStream` flattens all partitions into one stream — simple, but processing is serialized across partitions. For parallelism **with per-partition ordering preserved**, use `partitionedStream`:

```scala
ZIO.serviceWithZIO[Consumer](_
  .partitionedStream(Subscription.topics("orders"), Serde.string, Serde.string)
  .flatMapPar(Int.MaxValue) { case (topicPartition, partitionStream) =>
    partitionStream
      .mapZIO(r => handle(r.value).as(r.offset))
      .aggregateAsync(Consumer.offsetBatches)
      .mapZIO(_.commit)
  }
  .runDrain
)
```

`flatMapPar(Int.MaxValue)` is correct here: the number of inner streams is bounded by the partitions assigned to this consumer. Bound the work inside each partition instead if needed.

### The high-level API

When you only want to write the processing function:

```scala
Consumer.consumeWith(settings, Subscription.topics("orders"), Serde.string, Serde.string) { record =>
  handle(record.value)
}
```

Offsets are committed for you. Less control, far less to get wrong — a good default for simple consumers.

### Subscriptions

```scala
Subscription.topics("a", "b")
Subscription.pattern("orders-.*")
Subscription.manual("orders", partition = 0)          // no consumer group rebalancing
Subscription.manual(TopicPartition("orders", 0) -> 42L)  // start at a specific offset
```

## Producing

```scala
val producer: ZLayer[Any, Throwable, Producer] =
  ZLayer.scoped(Producer.make(ProducerSettings(List("localhost:9092"))))

Producer.produce("topic", key, value, Serde.string, Serde.string)      // awaits ack
Producer.produceAsync(...)                                            // returns UIO[RecordMetadata]
Producer.produceChunk(chunk, Serde.string, Serde.string)              // batched — much faster
```

As a stream sink:

```scala
sourceStream
  .map(v => new ProducerRecord("out", v.key, v.value))
  .via(Producer.produceAll(Serde.string, Serde.string))
  .runDrain
```

`produce` waits for the broker acknowledgement per record — fine for low volume, slow at scale. Use `produceChunk`/`produceAll` for throughput, `produceAsync` when you want to pipeline acks.

## Serdes

```scala
Serde.string / int / long / double / byteArray / uuid / boolean
Serde.string.asOption                                    // tolerate null payloads
mySerde.inmap(f)(g)                                      // pure transformation
Serde.string.inmapZIO(decode)(encode)                    // effectful, can fail
Serde.byteArray.asTry
```

JSON via zio-json:

```scala
implicit val codec: JsonCodec[Order] = DeriveJsonCodec.gen[Order]
val orderSerde: Serde[Any, Order] =
  Serde.string.inmapZIO(s => ZIO.fromEither(s.fromJson[Order]).mapError(new RuntimeException(_)))(o => ZIO.succeed(o.toJson))
```

Schema Registry / Avro: use `zio-kafka` with a Confluent serde wrapped via `Serde.fromKafkaSerde`, or the `zio-schema-avro` module.

**Deserialization failures kill the stream.** Make it explicit:

```scala
.plainStream(sub, Serde.string, orderSerde.asTry)
.mapZIO {
  case r if r.value.isSuccess => handle(r.value.get).as(r.offset)
  case r                      => toDeadLetter(r).as(r.offset)   // never silently drop
}
```

## Offsets & delivery semantics

- **At-least-once** (the default shape above): commit *after* processing. A crash replays; handlers must be idempotent.
- **At-most-once**: commit before processing. Rarely what you want.
- **Exactly-once** within Kafka: transactions.

```scala
val txProducer = TransactionalProducer.make(ProducerSettings(brokers), TransactionalProducerSettings("tx-id"))

ZIO.scoped {
  txProducer.flatMap { p =>
    p.createTransaction.flatMap { tx =>
      stream.mapZIO(r => tx.produce("out", k, v, Serde.string, Serde.string, Some(r.offset))).runDrain
    }
  }
}
```

Transactions only give exactly-once *within Kafka*. A side effect on an external system inside the transaction is not covered — design for idempotency regardless.

Manual control: `record.offset.commit`, `Consumer.commitOrRetry(schedule)`, `settings.withCommitTimeout(d)`.

## Rebalancing

```scala
settings
  .withRebalanceSafeCommits(true)       // wait for in-flight commits before giving up partitions
  .withMaxRebalanceDuration(30.seconds)
  .withRebalanceListener(RebalanceListener(
    onAssigned = tps => ZIO.logInfo(s"assigned $tps"),
    onRevoked  = tps => ZIO.logInfo(s"revoked $tps")
  ))
```

`rebalanceSafeCommits` is the setting that prevents duplicate processing during rebalances at the cost of a slower handover. Enable it unless you have measured a reason not to.

## Tuning

```scala
settings
  .withMaxPollRecords(500)
  .withFetchStrategy(FetchStrategy.QueueSizeBased(maxPartitionQueueSize = 1024))
  .withPartitionPreFetchBufferLimit(1024)
  .withProperty("max.partition.fetch.bytes", "1048576")
```

Throughput problems are usually one of: per-record commits, `plainStream` where `partitionedStream` was needed, unbounded `mapZIOPar` starving the poll loop, or blocking work on the async pool (wrap it in `ZIO.attemptBlocking`).

## Testing

- **Embedded/Testcontainers** is the honest option:
  ```scala
  val kafkaLayer = ZLayer.scoped(ZIO.acquireRelease(startKafkaContainer)(stopContainer))
  spec.provideShared(kafkaLayer) @@ TestAspect.sequential @@ TestAspect.withLiveClock
  ```
  Kafka needs the real clock — remember `withLiveClock`.
- **Unit-test the processing function**, not the consumer. Extract `handle(record) : ZIO[R, E, Unit]` and test it directly; the consumer wiring is library code.

## Checklist

- Commits batched, not per-record.
- Deserialization failures routed to a dead-letter path, not crashing the stream.
- Handlers idempotent (at-least-once is the realistic default).
- `partitionedStream` when you need parallelism; per-partition ordering preserved.
- `Consumer` and `Producer` created via `ZLayer.scoped` so they close cleanly.
- Blocking work inside handlers wrapped in `ZIO.attemptBlocking`.
- SASL/SSL properties loaded from `Config.Secret`, never hard-coded.
- Consumer lag monitored — expose it as a `Metric.gauge` and alert on it.

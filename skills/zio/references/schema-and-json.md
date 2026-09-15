# ZIO Schema & ZIO JSON

```scala
"dev.zio" %% "zio-json"             % V
"dev.zio" %% "zio-schema"           % V
"dev.zio" %% "zio-schema-derivation" % V   // DeriveSchema
"dev.zio" %% "zio-schema-json"      % V   // Schema -> JSON codec
"dev.zio" %% "zio-schema-protobuf"  % V   // Schema -> protobuf codec
"dev.zio" %% "zio-schema-avro"      % V
```

Two different things that are easy to confuse:

- **zio-json** is a fast JSON codec. Use it when JSON is the only format you need.
- **zio-schema** is a *reified description of a data type* — a value you can inspect at
  runtime. From one `Schema[A]` you get JSON, protobuf, Avro and Thrift codecs, plus
  migrations, optics, and dynamic transformation. Use it when you need more than one wire
  format, or when you need to evolve the format over time.

A Kafka `Serde` and an HTTP `Binary`/`Endpoint` body both need a codec. This file is the
glue that `references/kafka.md` and `references/http.md` assume exists.

## zio-json

```scala
import zio.json._

final case class Event(id: String, at: java.time.Instant, payload: Map[String, String])

object Event:
  given JsonCodec[Event] = DeriveJsonCodec.gen[Event]
```

`DeriveJsonCodec.gen` needs codecs for every field; primitives, collections, `Option`,
`Either`, `java.time.*`, `UUID` are built in. Split into `DeriveJsonEncoder.gen` /
`DeriveJsonDecoder.gen` when only one direction is needed.

Encoding and decoding:

```scala
event.toJson            // String
event.toJsonPretty
"""{"id":"1"}""".fromJson[Event]   // Either[String, Event]
```

`fromJson` returns `Either[String, A]` — a plain message, not an exception. Lift it:

```scala
ZIO.fromEither(raw.fromJson[Event]).mapError(DecodeError(_))
```

### Field customization

```scala
final case class User(
  @jsonField("user_id") id: String,
  @jsonExclude secret: String,
  @jsonAliases("mail") email: String
)
```

Useful annotations: `@jsonField` (rename), `@jsonExclude`, `@jsonAliases`,
`@jsonDiscriminator("type")` on a sealed trait, `@jsonNoExtraFields` (reject unknown
keys — worth it on external input), `@jsonMemberNames(SnakeCase)` at the type level.

### Sum types

```scala
@jsonDiscriminator("type")
sealed trait Command derives JsonCodec
final case class Create(name: String) extends Command
final case class Delete(id: String)   extends Command
```

Without a discriminator, zio-json uses the wrapper-object encoding
(`{"Create":{"name":"x"}}`). Pick one deliberately and keep it stable — changing it
breaks every existing consumer.

### Streaming JSON

zio-json integrates with ZStream for large payloads and newline-delimited JSON:

```scala
import zio.json._
stream.via(JsonDecoder[Event].decodeJsonPipeline(JsonStreamDelimiter.Newline))
```

This matters for anything unbounded — parsing a multi-GB file into memory is a defect
waiting to happen.

## zio-schema

```scala
import zio.schema._

final case class Event(id: String, at: java.time.Instant, amount: BigDecimal)

object Event:
  given Schema[Event] = DeriveSchema.gen[Event]
```

From that one value:

```scala
import zio.schema.codec._

val json  = JsonCodec.schemaBasedBinaryCodec[Event]
val proto = ProtobufCodec.protobufCodec[Event]
val avro  = AvroCodec.schemaBasedBinaryCodec[Event]
```

Each codec exposes `encode: A => Chunk[Byte]`, `decode: Chunk[Byte] => Either[DecodeError, A]`,
and stream pipelines (`streamEncoder` / `streamDecoder`) that plug straight into ZStream.

### Why a schema rather than a codec

- One definition, several wire formats — the usual case being JSON on the HTTP edge and
  protobuf or Avro on the Kafka topic.
- **Migrations.** `Schema.migrate(from, to)` derives a transformation between two
  versions of a type and tells you at runtime when it isn't possible. This is the reason
  to use zio-schema in an event-sourced or long-retention-topic system: consumers written
  against v1 must keep reading records written by v2.
- `DynamicValue` — a schema-directed generic representation, for when you must handle
  data whose type isn't known at compile time.
- Optics (`Lens`, `Prism`, `Traversal`) derived from the schema.

### Evolution rules that actually bite

- Adding a field with a default is backward compatible. Adding one without a default is
  not — old records have no value for it.
- Removing a field breaks consumers still reading it. Deprecate before removing.
- Renaming is removal plus addition. Use `@fieldName` to keep the wire name stable while
  the Scala name changes.
- Changing a type (`Int` to `Long`, `String` to enum) is a breaking change in every
  format, regardless of what the Scala compiler says.
- Protobuf and Avro identify fields positionally or by ID. **Do not reorder fields** in a
  case class whose schema backs a protobuf codec.

## Wiring codecs into the rest of the stack

### Kafka Serde

```scala
import zio.kafka.serde.Serde

given eventSerde: Serde[Any, Event] =
  Serde.string.inmapM[Any, Event](
    s => ZIO.fromEither(s.fromJson[Event]).mapError(e => new RuntimeException(e))
  )(e => ZIO.succeed(e.toJson))
```

For binary formats, go through `Serde.byteArray` and the schema-based codec instead.
Deserialization failures are the single most common cause of a stuck consumer — route
them to a dead-letter topic rather than letting them kill the stream. See
`references/kafka.md`.

### zio-http bodies

```scala
req.body.to[Event]              // needs a JsonCodec / Schema in scope
Response.json(event.toJson)
```

Declarative `Endpoint` definitions take the schema directly and generate the OpenAPI
document from it — see `references/http.md`.

## Testing

- Round-trip properties are the highest-value test here:
  ```scala
  check(eventGen) { e => assertTrue(e.toJson.fromJson[Event] == Right(e)) }
  ```
  `DeriveGen.gen[Event]` (zio-schema) produces the generator from the schema for free.
- Pin the wire format with a golden test: a literal JSON string checked against the
  decoded value. Derivation changes silently when you reorder or rename fields; a golden
  test is what catches it.
- Test decoding of *malformed* input explicitly. That's the path attackers use.

## Checklist

- `@jsonNoExtraFields` on types decoded from untrusted input
- Secrets and tokens not serializable — `@jsonExclude`, or keep them out of the DTO
- Decode errors mapped to a typed domain error, never `.get` or `.orDie` on the `Either`
- Discriminator strategy for sum types chosen explicitly and stable across releases
- Field order untouched in any type backing a protobuf/Avro codec
- Large or externally-sized payloads decoded via stream pipelines, not `fromJson` on a
  fully-buffered `String`
- Separate wire DTOs from domain types where the two evolve on different schedules —
  deriving a codec directly on a domain model couples your public format to a refactor

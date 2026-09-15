# Observability

Logging, metrics and tracing in ZIO 2. All three are built on the same idea: context
travels with the fiber, so annotate once at the edge and every downstream effect inherits
it.

```scala
"dev.zio" %% "zio-logging"                  % V
"dev.zio" %% "zio-logging-slf4j2"           % V   // bridge to an existing SLF4J setup
"dev.zio" %% "zio-metrics-connectors"       % V   // Prometheus, StatsD, Datadog, New Relic
"dev.zio" %% "zio-opentelemetry"            % V   // tracing (zio-telemetry)
```

## Logging

Core ZIO needs no dependency:

```scala
ZIO.logInfo("order accepted")
ZIO.logWarning("retrying")
ZIO.logError("rejected")
ZIO.logErrorCause("handler failed", cause)    // keeps the fiber trace
ZIO.logDebug("...")
```

Use `logErrorCause` whenever you have a `Cause`. `logError(cause.toString)` throws away
the fiber trace, which is the part that tells you where it actually happened.

### Annotations are the whole point

```scala
ZIO.logAnnotate("requestId", id) {
  handleRequest(req)     // every log line inside carries requestId, at any depth
}
```

Annotations propagate down the fiber tree, including through `forkDaemon` children and
across `ZStream` stages. Annotate once at the boundary — HTTP middleware, the Kafka
record handler, the job runner — and stop threading a correlation ID through function
signatures.

```scala
ZIO.logAnnotate(
  LogAnnotation("requestId", id),
  LogAnnotation("userId", user.id)
)(effect)
```

`ZIO.logSpan("db-query")(effect)` adds a named span with elapsed time to the log line.

### Configuring the logger

Replace the default logger in `bootstrap`, so it applies before anything runs:

```scala
import zio.logging.backend.SLF4J

object Main extends ZIOAppDefault:
  override val bootstrap =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j
```

For structured JSON logs without SLF4J, zio-logging provides
`consoleJsonLogger(LogFormat...)`. Structured output is what makes annotations useful —
they become queryable fields rather than text in a message.

Forgetting `removeDefaultLoggers` gives you every line twice.

### What not to log

- Never a whole `Request`/`Response`/`Headers` — `Authorization`, cookies and tokens live
  there. Log an explicit allowlist of fields.
- Never a case class containing credentials. `Config.Secret` protects against this by
  construction; a `String` password field does not.
- Don't log a defect at every frame on the way up. Log once at the edge with context,
  and let it propagate (`SKILL.md`, error model).

## Metrics

Metrics are values applied as aspects with `@@`, so they compose with the effect rather
than wrapping it in bookkeeping code.

```scala
import zio.metrics._

val ordersProcessed = Metric.counter("orders_processed")
val queueDepth      = Metric.gauge("queue_depth")
val handlerLatency  = Metric.timer("handler_latency", ChronoUnit.MILLIS)

handleOrder(o) @@ ordersProcessed @@ handlerLatency
```

| Type | Use for |
|---|---|
| `Metric.counter` | monotonic totals — requests, errors, retries |
| `Metric.gauge` | a current value — queue depth, consumer lag, pool in use |
| `Metric.histogram` | distribution with explicit boundaries |
| `Metric.summary` | quantiles over a sliding window |
| `Metric.frequency` | counts per string label — status codes, error kinds |

Tagging:

```scala
ordersProcessed.tagged("topic", topic).tagged("result", "ok")
```

Keep tag *cardinality* low. A tag carrying a user ID or a request ID will destroy your
metrics backend — that's what logs and traces are for.

`Metric.gauge` needs something to poll it: `metric.trackAll(value)` on a schedule, or
`ZIO.repeat` reading the source. A gauge nobody updates silently reports its last value
forever.

### Exporting

```scala
import zio.metrics.connectors.prometheus._

val metricsLayer = ZLayer.make[Unit](
  prometheusLayer,
  publisherLayer,
  ZLayer.succeed(MetricsConfig(5.seconds))
)
```

Then expose `/metrics` from your zio-http routes. JVM runtime metrics come from
`DefaultJvmMetrics.live` — add it, it's free and it's the first thing you want during an
incident.

## Tracing

zio-telemetry's OpenTelemetry module. `Tracing` is a service; its aspects come from
`tracing.aspects`:

```scala
import zio.telemetry.opentelemetry.tracing.Tracing

ZIO.serviceWithZIO[Tracing] { tracing =>
  import tracing.aspects._

  (handleRequest(req)
    @@ setAttribute("http.method", req.method.name)
    @@ span("handle-request"))
}
```

- `root("name")` starts a new trace; `span("name")` starts a child of the current one.
- Spans end when the effect completes — including on failure and interruption.
- Cross-process propagation uses `inject`/`extract` with a carrier (W3C
  `TraceContextPropagator` is the default choice). Inject on the way out of an HTTP
  client or into Kafka headers; extract at the receiving edge. These use mutable carrier
  APIs and are not referentially transparent — keep them at the boundary.

Configure `Tracing.scoped(tracer, ctxStorage, logAnnotated = true)` to copy trace and
span IDs into log annotations. That's what links a log line to a trace in your backend,
and it's the single highest-value piece of this whole file.

## The three together

A useful default at every boundary — HTTP handler, Kafka record, scheduled job:

```scala
def instrumented[R, E, A](name: String, id: String)(effect: ZIO[R, E, A]) =
  ZIO.logAnnotate("requestId", id) {
    effect
      .tapErrorCause(c => ZIO.logErrorCause(s"$name failed", c))
      @@ Metric.counter(s"${name}_total")
      @@ Metric.timer(s"${name}_latency", ChronoUnit.MILLIS)
  }
```

Rule of thumb: **metrics tell you something is wrong, traces tell you where, logs tell
you why.** Don't try to make one do another's job — high-cardinality metrics and
log-scraped dashboards are both symptoms of that mistake.

## Testing

- `ZTestLogger.logOutput` captures log entries so you can assert on them. Worth it for
  "we log a warning when the circuit opens" style requirements, not for every line.
- Metrics are inspectable: `metric.value` returns the current state, so
  `assertTrue(counterValue.count == 1)` works without a backend.
- `@@ TestAspect.silent` suppresses log noise in test output.

## Checklist

- `removeDefaultLoggers` applied when installing a custom logger (otherwise: duplicates)
- Logger configured in `bootstrap`, not inside `run`
- Correlation ID annotated once at each entry point, never threaded manually
- `logErrorCause` used wherever a `Cause` is available
- No request/response objects, headers, or secret-bearing types in log calls
- Metric tags low-cardinality; no IDs
- Gauges actually updated on a schedule
- JVM metrics registered
- Trace and span IDs linked into log annotations
- Span propagation injected at outbound edges and extracted at inbound ones

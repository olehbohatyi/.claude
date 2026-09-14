# ZIO HTTP

```scala
libraryDependencies += "dev.zio" %% "zio-http" % zioHttpVersion
import zio._, zio.http._
```

Check the current version at `https://index.scala-lang.org/zio/zio-http`. The API changed substantially before 3.0 — if you see `Http.collectZIO`, `HttpApp[R, E]` as a function type, or `App[R]`, that's the old API; the current model is `Routes` + `Handler`.

## Mental model

An HTTP transaction is a function `Request => ZIO[R, Response, Response]`. Both channels are `Response`, which forces you to decide what the client sees for every failure. Routes are stored in a prefix tree so dispatch cost depends on path depth, not route count.

Core types: `Request`, `Response`, `Body`, `Headers`, `Status`, `Method`, `URL`, `RoutePattern`, `Handler`, `Route`, `Routes`, `Middleware`, `HandlerAspect`.

## Minimal server

```scala
object Main extends ZIOAppDefault {
  val routes: Routes[Any, Response] = Routes(
    Method.GET / "health" -> handler(Response.ok),
    Method.POST / "echo"  -> handler { (req: Request) =>
      req.body.asString.map(Response.text(_))
    }.sandbox
  )

  def run = Server.serve(routes).provide(Server.default)
}
```

`Server.default` binds port 8080. Configure with `Server.defaultWithPort(9000)` or:

```scala
Server.live ++ ZLayer.succeed(Server.Config.default.port(9000).maxHeaderSize(16 * 1024))
```

## Route patterns

Path segments are typed; the handler's parameters are checked against them at compile time.

```scala
import zio.http.codec.PathCodec._

Method.GET / "users" / uuid("id") / "profile"           // RoutePattern[UUID]
Method.GET / "users" / int("id") / "posts" / string("slug")   // RoutePattern[(Int, String)]
Method.GET / "files" / trailing                          // rest of the path
Method.ANY / "proxy"
```

Codecs: `string`, `int`, `long`, `boolean`, `uuid`, `literal`, `trailing`, `empty`.

```scala
val route = Method.GET / "users" / uuid("id") -> handler { (id: UUID, req: Request) =>
  UserService.find(id).map(u => Response.json(u.toJson))
}
```

The handler **must** accept every path parameter plus the `Request`, in order, even if it ignores some.

## Handlers

```scala
trait Handler[-R, +Err, -In, +Out] { def apply(in: In): ZIO[R, Err, Out] }
```

```scala
handler(Response.text("hi"))                          // constant
handler { (req: Request) => ZIO.succeed(Response.ok) }
handler { (id: UUID, req: Request) => ... }
Handler.fromZIO(effect)
Handler.fromFunctionZIO[Request](req => ...)
Handler.fromFile(file) / fromResource("index.html") / fromStream(zstream)
Handler.notFound / ok / error(status)
```

Operators: `map`, `mapZIO`, `flatMap`, `catchAll`, `mapError`, `sandbox`, `orDie`, `timeout`, `@@ aspect`.

## Bodies

```scala
// reading
req.body.asString
req.body.asChunk                      // Chunk[Byte]
req.body.asStream                     // ZStream[Any, Throwable, Byte] — streaming upload
req.body.to[User]                     // needs a zio-schema Schema[User] in scope

// writing
Response.text("hi")
Response.json("""{"ok":true}""")
Response.html(Html.fromString(...))
Response(status = Status.Created, body = Body.from(user))     // Schema-based
Body.fromString / fromChunk / fromStream(stream) / fromFile(f) / fromMultipartForm(...)
```

`Body.from[A]` / `body.to[A]` require `zio.schema.Schema[A]`:

```scala
final case class User(id: UUID, name: String)
object User { implicit val schema: Schema[User] = DeriveSchema.gen[User] }
```

Streaming both ways is first class — `Body.fromStream` for large downloads, `req.body.asStream` for uploads; neither buffers the whole payload.

## Error handling

The server only accepts `Routes[R, Response]` — a route whose error channel is anything else won't compile. That's deliberate: the server must never crash on an unhandled error.

```scala
route.handleError(e => Response.badRequest(e.message))
routes.handleErrorCause(cause => Response.internalServerError)
routes.handleErrorRequestCause((req, cause) => ...)
routes.sandbox      // map every error/defect to a sensible status automatically
```

`sandbox` maps common exceptions to statuses (`FileNotFoundException` → 404, `IllegalArgumentException` → 400, `AccessDeniedException` → 403, everything else → 500). Good as a safety net at the edge of the app; **not** a substitute for deliberate mapping:

```scala
val userRoutes = Routes(
  Method.GET / "users" / uuid("id") -> handler { (id: UUID, _: Request) =>
    UserService.find(id).map(u => Response(body = Body.from(u)))
  }
).handleError {
  case UserNotFound(id)  => Response.notFound(s"no user $id")
  case UserInactive(_)   => Response.forbidden("inactive")
  case RepoUnavailable   => Response.status(Status.ServiceUnavailable)
}.sandbox   // catch anything unforeseen
```

**Security:** never let a raw exception message reach the body. `sandbox` on its own can leak paths and SQL fragments through `getMessage`. Map deliberately, log the cause server-side, return a generic message.

## Middleware & aspects

Two flavours:
- `Middleware` — sees the whole routing table, can add/remove/rewrite routes. More powerful, more overhead.
- `HandlerAspect` — wraps a handler, can't change routing. Cheaper; use by default.

```scala
routes @@ Middleware.cors(CorsConfig(allowedOrigins = _ => Some(Header.AccessControlAllowOrigin.Specific(origin))))
       @@ Middleware.requestLogging()
       @@ Middleware.metrics()
       @@ Middleware.timeout(30.seconds)
       @@ Middleware.serveDirectory(Path.empty / "static", dir)
       @@ Middleware.basicAuth("user", "pass")
       @@ Middleware.bearerAuth(verify)
       @@ Middleware.beautifyErrors
```

Custom aspect that injects an authenticated user into the environment:

```scala
val authenticate: HandlerAspect[AuthService, User] =
  HandlerAspect.interceptIncomingHandler(
    Handler.fromFunctionZIO[Request] { req =>
      req.header(Header.Authorization) match {
        case Some(Header.Authorization.Bearer(token)) =>
          AuthService.verify(token.value.asString)
            .map(user => (req, user))
            .orElseFail(Response.unauthorized)
        case _ => ZIO.fail(Response.unauthorized)
      }
    }
  )

val secured = Routes(
  Method.GET / "me" -> handler { (_: Request) =>
    ZIO.serviceWith[User](u => Response.text(u.name))
  }
) @@ authenticate
```

The aspect's output type becomes available in the handler's environment — this is the idiomatic way to thread request-scoped context.

## Declarative endpoints

Describe the contract once; get a typed server, a typed client and OpenAPI from it.

```scala
import zio.http.endpoint._
import zio.http.codec._

val getUser =
  Endpoint(Method.GET / "users" / uuid("id"))
    .out[User]
    .outError[UserNotFound](Status.NotFound)

val route = getUser.implement { id => UserService.find(id) }

val routes = Routes(route)

// client, derived from the same value
val client = EndpointExecutor(...)  // or getUser.toClient / ClientEndpoint

// OpenAPI
val openApi = OpenAPIGen.fromEndpoints(title = "API", version = "1.0", getUser)
val docs    = SwaggerUI.routes("docs", openApi)
```

Combinators: `.in[A]` (body), `.query(...)`, `.header(...)`, `.out[A]`, `.outError[E](status)`, `.outCodec`, `.examples`, `.tag`.

Use endpoints when you have a real API surface — the compile-time guarantee that client and server agree is worth the extra ceremony. Use plain `Routes` for a handful of internal routes.

## Client

```scala
val program = for {
  url  <- ZIO.fromEither(URL.decode("https://api.example.com/users"))
  res  <- Client.batched(Request.get(url).addHeader(Header.Authorization.Bearer(token)))
  body <- res.body.asString
} yield body

program.provide(Client.default)
```

- `Client.batched(req)` — buffers the full response.
- `Client.streaming(req)` — scoped, gives a streaming body; wrap in `ZIO.scoped`.
- `Client.default` for the standard layer; configure with `ZClient.Config` (connection pool, timeouts, SSL) via `ZLayer.succeed(...) >>> Client.live`.

Always bound client calls: `.timeout(...)`, a jittered retry `Schedule`, and a `Semaphore` if you fan out.

## WebSockets & SSE

```scala
val socket = Handler.webSocket { channel =>
  channel.receiveAll {
    case Read(WebSocketFrame.Text(t)) => channel.send(Read(WebSocketFrame.text(t.reverse)))
    case _                            => ZIO.unit
  }
}
val route = Method.GET / "ws" -> socket.toResponse

// server-sent events
Response.fromServerSentEvents(ZStream.repeatWithSchedule(ServerSentEvent("tick"), Schedule.spaced(1.second)))
```

## Testing routes

```scala
test("returns the user") {
  for {
    res  <- routes.runZIO(Request.get(URL(Path.root / "users" / id.toString)))
    body <- res.body.asString
  } yield assertTrue(res.status == Status.Ok, body.contains("ada"))
}.provide(UserServiceStub.layer)
```

`routes.runZIO(request)` exercises the routing table in-process — no port, no sockets. Use `TestClient`/a real `Server` layer only for integration tests.

## Checklist

- Every `Routes` reaching `Server.serve` handles its errors deliberately, with `sandbox` only as a backstop.
- CORS, CSRF and auth configured explicitly — the defaults are permissive-by-omission, not secure-by-default.
- Request body size limited (`Server.Config.maxHeaderSize`, and validate `Content-Length` / use streaming for large uploads).
- Timeouts on both server (`Middleware.timeout`) and client.
- Bodies parsed through `zio-schema`, not hand-rolled string handling.
- No secrets or stack traces in responses; log the `Cause` server-side with a correlation ID via `ZIO.logAnnotate`.

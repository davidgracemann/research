[![pipeline status](https://dbgit.prakinf.tu-ilmenau.de/code/sensbee/badges/main/pipeline.svg)](https://dbgit.prakinf.tu-ilmenau.de/code/sensbee/-/commits/main) [![coverage report](https://dbgit.prakinf.tu-ilmenau.de/code/sensbee/badges/main/coverage.svg)](https://dbgit.prakinf.tu-ilmenau.de/code/sensbee/-/commits/main)

# SensBee - Sensor Data Backend

SensBee is a robust IoT and Smart City sensor data platform written primarily in Rust. It provides centralised sensor registration, multi-protocol data ingestion (HTTP and MQTT), JavaScript-based data transformation, a granular role-based access control system, real-time event streaming, and an OpenTelemetry observability stack — all deployable as a Docker Compose stack.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Workspace Structure](#workspace-structure)
3. [Services & Components](#services--components)
   - [server — Main REST API](#server--main-rest-api)
   - [sensor_mgmt — Core Library](#sensor_mgmt--core-library)
   - [event_handling — Event Handler Service](#event_handling--event-handler-service)
   - [sbc — CLI Client](#sbc--cli-client)
   - [openapi — Swagger UI Server](#openapi--swagger-ui-server)
   - [Data Transform Service (Node.js)](#data-transform-service-nodejs)
   - [SBMI — Management Interface](#sbmi--management-interface)
   - [Supporting Infrastructure](#supporting-infrastructure)
4. [Database Schema](#database-schema)
   - [Core Tables](#core-tables)
   - [Dynamic Sensor Tables](#dynamic-sensor-tables)
   - [PostgreSQL Functions](#postgresql-functions)
5. [Data Flow](#data-flow)
   - [HTTP Ingestion](#http-ingestion)
   - [MQTT Ingestion](#mqtt-ingestion)
   - [Data Transformation Pipeline](#data-transformation-pipeline)
   - [Event Engine (LES)](#event-engine-les)
   - [Live Event Stream (WebSocket)](#live-event-stream-websocket)
6. [Authentication](#authentication)
   - [JWT Cookie Auth](#jwt-cookie-auth)
   - [API Keys](#api-keys)
   - [OpenID Connect](#openid-connect)
7. [Access Control (RBAC)](#access-control-rbac)
8. [Configuration](#configuration)
9. [Telemetry & Observability](#telemetry--observability)
10. [REST API Reference](#rest-api-reference)
11. [Dependencies](#dependencies)
12. [Building & Running](#building--running)
13. [Running Tests](#running-tests)
14. [Contributing](#contributing)
15. [License](#license)

---

## Architecture Overview

```
                          +------------------+
  Sensor (HTTP/MQTT) ---> |  Actix-Web REST  | <--- SBMI (browser UI)
                          |  server (8080)   |
                          +--------+---------+
                                   |
                    +--------------+--------------+
                    |              |              |
             +------v------+  +---v---+   +------v------+
             | sensor_mgmt |  | sqlx  |   |  OpenTel.   |
             | library     |  | PgPool|   |  Telemetry  |
             +------+------+  +---+---+   +------+------+
                    |              |              |
          +---------+------+  +---v---+   +------v------+
          |                |  |  PG   |   | OTel        |
   +------v-----+  +-------v+ | 5432  |   | Collector   |
   | Transform  |  | MQTT   | +-------+   | (4318)      |
   | Service    |  | Sub    |             +------+------+
   | (WS 9002)  |  | (WS)   |                    |
   | Node.js    |  | rumqttc|             +------v------+
   | isolate-vm |  +---+----+             | Grafana     |
   +------------+      |                  | Stack       |
                  +-----v------+          +-------------+
                  | Mosquitto  |
                  | MQTT 1883  |
                  | WS   9001  |
                  +------------+

   Standalone:
   +---------------------------+
   | sb_event_handler          |
   | Listens pg_notify, runs   |
   | registered task handlers  |
   +---------------------------+

   CLI:
   +---------------------------+
   | sbc                       |
   | Interactive REPL client   |
   | + Ollama LLM integration  |
   +---------------------------+
```

---

## Workspace Structure

```
sensbee-main/
├── Cargo.toml               # Workspace manifest (resolver = "2")
├── Dockerfile               # Multi-stage: server, event_handler, ci_test
├── docker-compose.yml       # Development stack
├── docker-compose.prod.yml  # Production stack
├── .env                     # Environment variable defaults
├── .sqlx/                   # Offline sqlx query cache (11 files)
├── migrations/
│   └── 20251009121900_init.up.sql   # Full schema + stored procedures
├── config/                  # Runtime config dir (mounted into container)
│   └── config.yml           # YAML server configuration (optional)
├── server/                  # Binary: sb_srv
│   └── src/main.rs
├── sensor_mgmt/             # Library: sensor_mgmt
│   └── src/
│       ├── lib.rs
│       ├── state.rs         # AppState (shared across all request handlers)
│       ├── utils.rs
│       ├── authentication/  # JWT, API key, OpenID Connect
│       ├── database/        # sqlx queries + DB models
│       ├── features/        # Cache, config, telemetry, transforms, events
│       └── handler/         # Actix-Web route handlers
├── event_handling/          # Binary: sb_event_handler
│   ├── src/
│   │   ├── main.rs
│   │   └── tasks/
│   ├── tasks_lib/           # Library: task runner framework
│   └── tasks_lib_macro/     # Proc-macro: #[task] attribute
├── sbc/                     # Binary: sbc (CLI client)
│   └── src/
├── openapi/                 # Binary: sb_openapi (Swagger UI)
│   └── src/main.rs
├── services/
│   ├── node.js/             # Data transform microservice
│   │   ├── app.js           # WebSocket server + isolated-vm runner
│   │   └── package.json
│   ├── sbmi/                # SensBee Management Interface (static JS)
│   │   ├── static/          # HTML/CSS/JS served by Nginx
│   │   └── nginx/
│   ├── mosquitto/           # Mosquitto broker config
│   └── grafana/             # OTel Collector config + Grafana stack
└── tools/
    └── upload_size_test.sh
```

---

## Services & Components

### server — Main REST API

**Crate:** `sb_srv` (`server/src/main.rs`)
**Runtime:** Actix-Web 4, Tokio async runtime
**Default port:** `8080`

The entry point of the platform. On startup it:

1. Prints the SensBee ASCII banner and version/compile-time metadata (via `compile-time` crate).
2. Initialises OpenTelemetry (traces, logs, metrics) over OTLP HTTP.
3. Connects to PostgreSQL using a connection pool (`PgPoolOptions`, up to 20 connections).
4. Runs all pending sqlx migrations from the `migrations/` directory.
5. Builds the shared `AppState` (cache, MQTT listener, event engine, transform service, OAuth state, JWT config).
6. In development mode (`run_mode: dev`), optionally auto-creates and elevates a root user.
7. Starts the Actix-Web `HttpServer` with:
   - `EventGenerator` middleware — intercepts every request/response and emits a `LogEvent` to the internal event channel.
   - `TracingLogger` middleware — structured tracing with OpenTelemetry context propagation.
   - CORS configured to allow any origin with `GET`, `POST`, `DELETE` and standard headers.
   - Payload size limit configurable via `ingest_max_size_kb` (default 256 KB).

### sensor_mgmt — Core Library

**Crate:** `sensor_mgmt` (library, shared by `server`, `event_handling`, `sbc`, `openapi`)

The heart of the system. Organised into six top-level modules:

#### `authentication/`

| File | Responsibility |
|------|---------------|
| `jwt_auth.rs` | JWT extraction from cookies, validation against RS256 public key, user identity resolution |
| `token.rs` | Token generation (RS256 private key), cookie creation |
| `token_cache.rs` | Short-lived in-memory token cache to avoid repeated DB lookups |
| `openid.rs` | OpenID Connect client initialisation, auth-code flow, callback handling via `openidconnect` crate |

#### `database/`

All queries use `sqlx` with compile-time verification. The `.sqlx/` directory holds the offline query cache so the project builds without a live database (`SQLX_OFFLINE=true`).

| File | Tables accessed |
|------|----------------|
| `data_db.rs` | Dynamic sensor tables (`s_<id>`) — insert, query, delete sensor readings |
| `data_chain_db.rs` | `sensor_data_chain`, `sensor_data_chain_outbound` |
| `data_transformer_db.rs` | `data_transformer` |
| `event_handler_db.rs` | `event_handler` |
| `sensor_db.rs` | `sensor`, `sensor_schema` |
| `sensor_events_db.rs` | `log_events` |
| `user_db.rs` | `users`, `users_oidc`, `user_roles` |
| `role_db.rs` | `roles`, `user_roles`, `sensor_permissions` |
| `models/` | Rust structs mirroring all DB types |

#### `features/`

| File | What it does |
|------|-------------|
| `cache.rs` | In-memory read-through cache for sensors and API keys (reduces DB round-trips on hot ingest path) |
| `cache_sync.rs` | Optional feature `cache_sync` — synchronises cache across instances |
| `config.rs` | Parses `config/config.yml` (YAML via `serde_yml`), resolves env vars for DB connection, exposes typed getters for all settings |
| `event_generation.rs` | `EventGenerator` Actix middleware + background Tokio task that consumes `LogEvent`s from an unbounded channel, persists them to `log_events`, and issues `pg_notify` to notify downstream listeners |
| `sensor_col_ingest.rs` | Handles `INCREMENTAL` column ingest mode (values are added to the previous reading rather than stored as absolutes) |
| `sensor_data_storage.rs` | Ring-buffer storage policies (count-based and interval-based) implemented as PostgreSQL triggers |
| `sensor_data_transform.rs` | Manages the WebSocket connection to the Node.js transform service; caches compiled script references by `data_transformer` UUID; drives the request/response protocol |
| `telemetry.rs` | Initialises OpenTelemetry SDK (traces, logs, metrics), wires `tracing-subscriber` layers for stdout + OTel export |
| `user_sens_perm.rs` | Evaluates user-sensor permission requirements (INFO / READ / WRITE) by joining `user_roles` and `sensor_permissions` |

#### `handler/`

All handlers are annotated with `utoipa` for automatic OpenAPI schema generation.

| Handler | Routes |
|---------|--------|
| `auth_hdl.rs` | `POST /auth/login`, `POST /auth/logout`, `GET /auth/available`, `GET /auth/callback` |
| `sensor_hdl.rs` | `GET /api/sensors`, `GET/POST/PUT/DELETE /api/sensors/{id}`, API key CRUD, data chain CRUD |
| `data_hdl.rs` | `GET /api/sensors/{id}/data/load`, `DELETE /api/sensors/{id}/data/delete` |
| `data_ingest/http.rs` | `POST /api/sensors/{id}/data/ingest` |
| `data_ingest/mqtt.rs` | Background MQTT subscriber (not an HTTP route) |
| `data_ingest/ingest.rs` | `ingest_data_buisness_logic` — shared access control + transform + DB insert logic |
| `data_transform_hdl.rs` | CRUD for `data_transformer` scripts |
| `event_handler_hdl.rs` | CRUD for `event_handler` webhook targets |
| `live_events_hdl.rs` | `GET /api/les/v1/stream/ws` — WebSocket live event stream |
| `user_hdl.rs` | User registration, verification, info, edit, delete, role assignment/revocation |
| `role_hdl.rs` | Role CRUD |
| `policy.rs` | Central permission-check functions called by all handlers |
| `main_hdl.rs` | `GET /api/healthchecker`, `GET /api/` — registers all routes into Actix service config |

#### `state.rs`

`SharedState` (wrapped in `Arc<SharedState>` as `AppState`) holds all runtime state:

```
SharedState {
    is_prod: bool,
    db: PgPool,
    cache: Arc<CachedData>,
    data_transform: Arc<TransformService>,   // WebSocket client to Node.js
    mqtt_listener: Option<Arc<MQTT>>,        // Background MQTT subscriber
    events: Option<Arc<EventEngineState>>,   // Unbounded channel sender for LogEvents
    rt_stats: RuntimeIngestStats,            // In-memory ingest statistics
    cfg: Arc<ServerConfig>,                  // Parsed config.yml
    jwt: Arc<JWTConfig>,                     // RS256 key pair + max_age
    oauth: Arc<OAuthState>,                  // OpenID Connect clients
}
```

State is cloned cheaply (all fields are `Arc`-wrapped) and shared across all request handler invocations.

---

### event_handling — Event Handler Service

**Crate:** `sb_event_handler` (`event_handling/src/main.rs`)

A standalone binary that runs alongside the main server and reacts to system events published via PostgreSQL `NOTIFY`. Its task runner framework is split into two sub-crates:

#### `tasks_lib`

Defines the core abstractions:

- `InternalState` — holds only a `PgPool`; lighter than the full `AppState`.
- `EventHandlerTask` — a function pointer type `fn(SharedState) -> Pin<Box<dyn Future<...>>>`.
- `TASK_REGISTRY` — a `linkme::distributed_slice` static array automatically populated at link time.
- `spawn_tasks()` — iterates `TASK_REGISTRY` and spawns each task as a Tokio task wrapped in a watchdog that restarts the task on error with exponential backoff (up to 5 s).
- `run_task()` — the watchdog: on `Ok` it stops, on `Err` it logs and restarts with doubling delay.

#### `tasks_lib_macro`

A proc-macro crate providing the `#[task]` attribute. Applying it to an `async fn` registers that function into `TASK_REGISTRY` at link time using `linkme::distributed_slice`. This means adding a new event handler requires only:

```rust
#[task]
async fn my_handler(state: SharedState) -> Result<(), Box<dyn Error + Send + Sync>> {
    // listen on pg_notify channel, react to events
}
```

No manual registration is needed.

---

### sbc — CLI Client

**Crate:** `sbc` (`sbc/src/sbc.rs`)

An interactive terminal client for the SensBee API built with `rustyline` (readline-style input with bracket highlighting and history).

**Commands:**

| Command | Sub-commands | Description |
|---------|-------------|-------------|
| `user` | list, create, verify, delete, info | Manage user accounts |
| `role` | list, create, delete, assign, revoke | Manage roles |
| `sensor` | list, create, edit, delete, keys, chain | Manage sensors |
| `ingest` | (various) | Push test data to sensors |
| `data` | load, delete | Query or delete sensor readings |
| `ask <question>` | — | Ask the local Ollama LLM to generate a command |

**Authentication:** Logs in via `POST /auth/login`, extracts the JWT from the `set-cookie` response header, and passes it on all subsequent requests.

**Ollama integration:** The `ask` command sends the natural-language question to a local Ollama instance (via `ollama-rs`), which returns a SensBee CLI command that is then added to the readline history.

**Script mode:** Pass `--file <path>` to execute a semicolon-delimited script file non-interactively.

---

### openapi — Swagger UI Server

**Crate:** `sb_openapi` (`openapi/src/main.rs`)

A small Actix-Web binary that serves the Swagger UI (`utoipa-swagger-ui`) populated from the OpenAPI schema generated via `utoipa` annotations across the `sensor_mgmt` handlers. Used for interactive API exploration.

---

### Data Transform Service (Node.js)

**Location:** `services/node.js/app.js`
**Runtime:** Node.js 24 (via nvm)
**Port:** `9002` (WebSocket)
**Dependencies:** `isolated-vm ^6.0.0`, `ws ^8.18.2`

This microservice executes user-defined JavaScript transformation scripts on incoming sensor data in a sandboxed V8 environment. The Rust server connects to it over a persistent WebSocket.

#### Protocol (4 message types)

| Type ID | Name | Direction | Payload |
|---------|------|-----------|---------|
| 1 | `ERR_RESP` | Service → Rust | Error string |
| 2 | `TRANSFORM_REQUEST` | Rust → Service / Service → Rust | Input JSON / transformed JSON |
| 3 | `SCRIPT_REQ` | Service → Rust | Request for script source |
| 4 | `SCRIPT_RESP` | Rust → Service | Script source code |

#### Execution flow

1. Rust sends a `TRANSFORM_REQUEST` with a `script_id` (UUID of the `data_transformer` record) and the raw JSON data.
2. If the script is not yet compiled, the service sends a `SCRIPT_REQ` back to Rust.
3. Rust responds with a `SCRIPT_RESP` containing the JavaScript source.
4. The service compiles the script into a `ReusableScriptRunner` and caches it by `script_id`.
5. For each run, a fresh V8 context is created from the cached compiled script, `data` is injected as a global, and the script executes within a 1 second timeout.
6. The result (JSON-serialised) is returned as a `TRANSFORM_REQUEST` response.

The `isolated-vm` V8 sandbox prevents any user script from accessing Node.js APIs, the filesystem, network, or the host process.

---

### SBMI — Management Interface

**Location:** `services/sbmi/static/`
**Served by:** Nginx on port `8082`

A pure-JavaScript single-page application served as static files. State management is done entirely in the browser's `localStorage`. It communicates with the SensBee REST API directly from the browser.

To reset to the login screen:
```js
localStorage.clear()
```

---

### Supporting Infrastructure

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| `sb-postgres` | `postgres:17-alpine` | 5432 | Primary PostgreSQL database |
| `sb-service-mosquitto` | `eclipse-mosquitto:2.0` | 1883 (TCP), 9001 (WS) | MQTT broker |
| `sb-service-otel-collector` | `otel/opentelemetry-collector-contrib` | 4318 | OpenTelemetry Collector |
| `sb-service-SBMI` | `nginx:alpine` | 8082 | Static management UI |

The OTel Collector bridges between the application's OTLP HTTP exports and an external Grafana stack, connected via the `otel-net` Docker network.

---

## Database Schema

### Core Tables

#### `users`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `uuid` | PK, auto-generated |
| `name` | `varchar(100)` | Display name |
| `email` | `varchar(255)` | Unique, indexed |
| `verified` | `boolean` | Must be `true` for login to succeed |

#### `sensor`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `uuid` | PK |
| `name` | `varchar(50)` | Unique human-readable name |
| `tbl_name` | `varchar(50)` | Name of the corresponding dynamic data table |
| `longitude`, `latitude` | `double precision` | Physical location |
| `description` | `text` | Free-form description |
| `owner` | `uuid` | FK to `users` |
| `storage_type` | `varchar(50)` | `Default`, or ring-buffer variant |
| `storage_params` | `text` | Ring-buffer parameters |

#### `sensor_schema`
| Column | Type | Notes |
|--------|------|-------|
| `sensor_id` | `uuid` | FK to `sensor` |
| `col_name` | `varchar(50)` | Column name in the dynamic table |
| `col_type` | `integer` | 1 = INT, 2 = FLOAT, 3 = STRING |
| `col_unit` | `varchar(10)` | Physical unit (optional) |
| `col_ingest` | `integer` | 0 = LITERAL, 1 = INCREMENTAL |

#### `roles`
Four built-in system roles (cannot be created/deleted by API):

| ID | Name | Description |
|----|------|-------------|
| `0e804d35-...` | Admin | Manage users, sensors, roles |
| `72122092-...` | User | Standard user |
| `51fd9bb7-...` | Guest | Read-only access |
| `54344b08-...` | Root | Full system access |

Custom roles can be created via `POST /api/roles`.

#### `sensor_permissions`
Per-sensor, per-role permission flags: `allow_info`, `allow_read`, `allow_write`.

#### `api_keys`
Per-sensor keys with a single `operation` field: `READ` or `WRITE`. Passed as `?key=<uuid>` in HTTP or as a topic segment in MQTT.

#### `data_transformer`
Stores JavaScript source code with versioning (`version` integer incremented on each update).

#### `event_handler`
Webhook configuration: `url` (HTTP endpoint) + `method` (`GET` or `POST`).

#### `sensor_data_chain`
One inbound `data_transformer` per sensor — applied to every incoming data payload before DB insertion.

#### `sensor_data_chain_outbound`
Links a sensor to an optional `data_transformer` and a required `event_handler`. When data arrives for that sensor, the transformer is applied and the result is POSTed to the event handler URL.

#### `log_events`
| Column | Type | Notes |
|--------|------|-------|
| `t` | `timestamp` | Event time |
| `sensor_id` | `uuid` | Optional reference |
| `data` | `jsonb` | Full `LogEvent` serialised as JSON |

#### `users_oidc`
Stores `iss` (issuer) and `sub` (subject) claims from OpenID Connect tokens, linked to `users.id`.

### Dynamic Sensor Tables

When a sensor is created, a dedicated table is created: `s_<sensor_id_no_dashes>` (e.g., `s_5401df3b93674e8bb2dd6dba2b6cade6`).

Columns: `created_at TIMESTAMP` + one column per entry in `sensor_schema`.

Column types map as: `col_type = 1` → `INTEGER`, `2` → `DOUBLE PRECISION`, `3` → `TEXT`.

### PostgreSQL Functions

| Function | Purpose |
|----------|---------|
| `add_values(a, b)` | Overloaded for `double precision`, `integer`, `text` — null-safe addition/concatenation used by incremental ingest triggers |
| `hashed_col_name(tbl, col, lmt)` | Returns `tbl || SUBSTRING(md5(col), 1, lmt)` — used to generate short unique trigger/function names within PostgreSQL's 63-byte identifier limit |
| `create_ring_buffer_count(tbl, num)` | Installs an AFTER INSERT trigger that deletes all but the `num` most recent rows (count-based ring buffer) |
| `create_ring_buffer_interval(tbl, interval_min)` | Installs an AFTER INSERT trigger that deletes rows older than `interval_min` minutes (time-based ring buffer) |
| `create_sensor_column_ingest_incremental(tbl, col)` | Installs a BEFORE INSERT trigger that adds the new value to the previous one for incremental ingest columns |
| `remove_sensor_column_ingest_incremental(tbl, col)` | Drops the incremental ingest trigger/function for the given column |
| `senor_id_to_table_name(sensor_id)` | Returns `s_` + UUID with hyphens removed |

---

## Data Flow

### HTTP Ingestion

```
POST /api/sensors/{sensor_id}/data/ingest?key={api_key}
Body: [ { "col1": 42, "col2": 1.5, ... }, ... ]

1. EventGenerator middleware records request start time + OTel context
2. http::ingest_sensor_data_handler extracts sensor_id, api_key from path/query
3. ingest_data_buisness_logic:
   a. Looks up api_key in in-memory cache (falls back to DB)
   b. Checks key.sensor_id == sensor_id AND key.operation == WRITE
   c. If no key: checks sensor_permissions for public WRITE access
   d. Rejects with 401 if unauthorised
   e. Validates data.len() > 0
   f. Retrieves sensor metadata from cache
   g. Calls sensor_data_transform::transform()  ← optional Node.js round-trip
   h. Calls data_db::add_sensor_data()          ← INSERT into s_<id>
4. EventGenerator sends LogEvent to internal channel
5. log_event_db_relay persists LogEvent + pg_notify
```

### MQTT Ingestion

```
Topic: /api/sensors/{sensor_id}/[{api_key}]
Payload: JSON bytes

1. rumqttc WebSocket client connects to Mosquitto on ws://mosquitto:9001/mqtt
2. Subscribes to wildcard: /api/sensors/#
3. On Incoming::Publish:
   a. split_topic() extracts sensor_id and optional api_key from topic string
   b. Calls same ingest_data_buisness_logic as HTTP
   c. Updates per-sensor runtime stats (recv, err, err_auth, db_insert_succ)
4. On connection error: exponential backoff (1s → 2s → 4s ... max 30s), then reconnect
```

MQTT subscriber is initialised inside `AppState::new()` and runs as a detached Tokio task with a watchdog loop.

### Data Transformation Pipeline

When a sensor has an inbound `data_transformer` configured:

```
Raw JSON bytes
    |
    v
sensor_data_transform::transform()
    |
    +-- Check sensor.inbound_dt_id exists in cache
    |
    +-- Send WS message to Node.js (type=2, script_id=<dt_uuid>, data=<json>)
    |
    +-- If Node.js doesn't have script:
    |       Node.js sends SCRIPT_REQ (type=3)
    |       Rust fetches script source from data_transformer table
    |       Rust sends SCRIPT_RESP (type=4)
    |       Node.js compiles + caches V8 script
    |
    +-- Node.js runs script in fresh V8 context (1s timeout, 128MB memory limit)
    |   global `data` = input JSON
    |   script must return the transformed value
    |
    +-- Node.js sends TRANSFORM_REQUEST response (type=2) with result JSON
    |
    v
Transformed JSON bytes → add_sensor_data()
```

If no transformer is configured, raw bytes go directly to `add_sensor_data()`.

### Event Engine (LES)

The Log & Event Service runs as a background Tokio task:

```
Any handler / MQTT subscriber
    |
    v (tokio::sync::mpsc::unbounded_channel)
log_event_db_relay task
    |
    +-- default_filter(): only passes sensor data manipulation paths
    |   (ingest, delete, MQTT)
    |
    +-- extract_sensor_uuid() from request path
    |
    +-- INSERT INTO log_events(t, sensor_id, data)
    |
    +-- pg_notify("sensor/<uuid>" OR "log_events_general", <json>)
    |
    +-- Logs at info/warn/error level depending on HTTP status code
```

The `sb_event_handler` service listens on these PostgreSQL NOTIFY channels and triggers registered task handlers.

### Live Event Stream (WebSocket)

Endpoint: `GET /api/les/v1/stream/ws`

Clients connect and receive real-time `LogEvent` JSON messages as they are produced. Powered by Actix-WS and `pg_notify` consumption.

---

## Authentication

### JWT Cookie Auth

1. `POST /auth/login` with `{ "email": "...", "password": "..." }`
2. Server verifies credentials, generates an RS256 JWT (default expiry: 43200 minutes = 30 days), sets it as an HttpOnly cookie named `token`.
3. Subsequent requests include the cookie automatically. Handlers extract and validate it.
4. `POST /auth/logout` clears the cookie.

**Key management:** By default, a development key pair is embedded in the binary. For production, place custom PEM files at:
- `config/jwt/key.pem` (PKCS#8 RSA private key)
- `config/jwt/key.pub.pem` (RSA public key)

If running inside a Docker Compose stack (`SB_CONTAINER` env var set) without custom keys, the server exits with an error.

### API Keys

Generated per-sensor via `POST /api/sensors/{id}/keys`. Each key is a UUID with a `READ` or `WRITE` operation scope. Used in:
- HTTP: `?key=<uuid>` query parameter
- MQTT: `/api/sensors/{sensor_id}/{api_key}` topic path

Keys are cached in memory to reduce DB lookups on the hot ingest path.

### OpenID Connect

Configure one or more OIDC providers in `config/config.yml`:

```yaml
auth:
  oidc_clients:
    - name: "My Provider"
      client_id: "..."
      client_secret: "..."
      issuer_url: "https://accounts.example.com"
  default_verified: true
  root_user_email: "admin@example.com"
```

- `GET /auth/available` — returns list of configured OIDC provider names and their auth URLs.
- Provider redirects to `GET /auth/callback` with the auth code.
- On first login, a new `users` record is created (optionally pre-verified). The OIDC `sub`+`iss` pair is stored in `users_oidc`.
- If `root_user_email` matches the authenticated email and `run_mode: dev`, the Root and Admin roles are assigned automatically.

---

## Access Control (RBAC)

Permission resolution for any API call:

```
Request comes in with JWT token  (or no auth for public endpoints)
    |
    v
Extract user from JWT → look up user_roles
    |
    v
For sensor endpoints: join user_roles × sensor_permissions
    |
    +-- allow_info  → can read sensor metadata
    +-- allow_read  → can read sensor data
    +-- allow_write → can ingest data
    |
    v
policy::require_sensor_permission() returns None (allowed) or Some(AppError::Unauthorized)
```

System roles (`Admin`, `Root`) bypass sensor-level permission checks for management operations. The `Root` role is the only role that can assign/revoke other system roles.

---

## Configuration

### `config/config.yml`

```yaml
server:
  host: "0.0.0.0"          # Bind address (default: localhost)
  port: 8080                # Bind port (default: 8080)
  external_host: "http://localhost:8080"    # Used for OAuth redirect URIs
  external_sbmi_host: "http://localhost:8082"
  run_mode: "dev"           # "dev" enables dev-mode features; anything else = prod
  ingest_max_size_kb: 256   # Max HTTP request body size for ingest

auth:
  jwt:
    max_age: 43200          # JWT expiry in minutes
  oidc_clients:
    - name: "..."
      client_id: "..."
      client_secret: "..."
      issuer_url: "..."
  default_verified: false   # Auto-verify OIDC users
  root_user_email: "..."    # Auto-assign Root role to this email in dev mode
```

### Environment Variables (`.env`)

```
PSQL_USER=sensbee
PSQL_PASSWORD=supersecretpw
PSQL_DATABASE=sensbee_db
DATABASE_URL=postgres://${PSQL_USER}:${PSQL_PASSWORD}@localhost:5432/${PSQL_DATABASE}
TZ=Europe/Berlin
SB_CONTAINER=1             # Set this inside Docker to enable service hostnames
```

---

## Telemetry & Observability

All three OpenTelemetry signals are exported over OTLP HTTP to the collector:

| Signal | Endpoint |
|--------|---------|
| Traces | `http://sb-service-otel-collector:4318/v1/traces` |
| Logs | `http://sb-service-otel-collector:4318/v1/logs` |
| Metrics | `http://sb-service-otel-collector:4318/v1/metrics` |

The OTel Collector (configured in `services/grafana/otel-collector-config.yaml`) forwards data to an external Grafana stack over the `otel-net` Docker network.

`tracing-actix-web` (`TracingLogger`) integrates HTTP request tracing into the span hierarchy. OTel context is propagated through the `EventGenerator` middleware so that `LogEvent`s carry the correct trace ID.

Noisy internal crates (`hyper`, `tonic`, `h2`, `reqwest`) are suppressed from the OTel log layer to prevent telemetry-induced-telemetry loops.

---

## REST API Reference

A full interactive reference is available via the OpenAPI service (`sb_openapi` binary, backed by `utoipa` + `utoipa-swagger-ui`).

Quick summary of routes registered in `main_hdl.rs`:

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/api/` | — | Liveness probe |
| GET | `/api/healthchecker` | — | Health check, returns status message |
| GET | `/api/sensors` | Token | List all accessible sensors |
| POST | `/api/sensors` | Token | Create a new sensor |
| GET | `/api/sensors/{id}` | Token | Get sensor metadata |
| PUT | `/api/sensors/{id}` | Token | Edit sensor metadata |
| DELETE | `/api/sensors/{id}` | Token | Delete sensor |
| POST | `/api/sensors/{id}/keys` | Token | Generate API key |
| DELETE | `/api/sensors/{id}/keys/{key_id}` | Token | Delete API key |
| GET | `/api/sensors/{id}/chain` | Token | Get inbound data chain |
| POST | `/api/sensors/{id}/chain` | Token | Set inbound data chain |
| DELETE | `/api/sensors/{id}/chain` | Token | Delete inbound data chain |
| POST | `/api/sensors/{id}/data/ingest` | Key/Public | Ingest data |
| GET | `/api/sensors/{id}/data/load` | Token/Public | Query data |
| DELETE | `/api/sensors/{id}/data/delete` | Token | Delete data |
| GET | `/api/les/v1/stream/ws` | Token | WebSocket live event stream |
| GET | `/api/transformers` | Token | List data transformers |
| GET | `/api/transformers/{id}` | Token | Get transformer |
| POST | `/api/transformers` | Token | Create transformer |
| PUT | `/api/transformers/{id}` | Token | Update transformer |
| DELETE | `/api/transformers/{id}` | Token | Delete transformer |
| GET | `/api/event-handlers` | Token | List event handlers |
| GET | `/api/event-handlers/{id}` | Token | Get event handler |
| POST | `/api/event-handlers` | Token | Create event handler |
| DELETE | `/api/event-handlers/{id}` | Token | Delete event handler |
| GET | `/api/users` | Token | List users |
| POST | `/api/users/register` | — | Register user |
| POST | `/api/users/{id}/verify` | Token (Admin) | Verify user |
| GET | `/api/users/me` | Token | Get own user info |
| PUT | `/api/users/me` | Token | Edit own user info |
| DELETE | `/api/users/{id}` | Token (Admin) | Delete user |
| POST | `/api/users/{id}/roles/{role_id}` | Token (Admin) | Assign role |
| DELETE | `/api/users/{id}/roles/{role_id}` | Token (Admin) | Revoke role |
| GET | `/api/roles` | Token | List roles |
| POST | `/api/roles` | Token (Admin) | Create role |
| DELETE | `/api/roles/{id}` | Token (Admin) | Delete role |
| POST | `/auth/login` | — | Log in (returns JWT cookie) |
| POST | `/auth/logout` | — | Log out (clears cookie) |
| GET | `/auth/available` | — | List OIDC providers |
| GET | `/auth/callback` | — | OIDC auth-code callback |

---

## Dependencies

### System Requirements (Arch Linux / CachyOS)

| Package | Version | Purpose |
|---------|---------|---------|
| `rustc` / `cargo` | 1.94.0+ | Rust compiler and build tool |
| `openssl` | 3.6.1+ | TLS (required by `native-tls` feature of sqlx/reqwest) |
| `pkgconf` | 2.5.1+ | pkg-config for linking OpenSSL |
| `node` (nvm) | 24.x | Data transform microservice runtime |
| `npm` | 11.x | Node.js package manager |

### Rust Crates (key dependencies)

| Crate | Version | Role |
|-------|---------|------|
| `actix-web` | 4 | Async HTTP server framework |
| `actix-ws` | 0.4 | WebSocket support |
| `sqlx` | 0.8.3 | Async PostgreSQL driver with compile-time query verification |
| `tokio` | 1.44 | Async runtime |
| `rumqttc` | 0.25.1 | MQTT client (WebSocket transport) |
| `jsonwebtoken` | 10.3 | RS256 JWT signing and verification |
| `openidconnect` | 4.0.1 | OpenID Connect auth-code flow |
| `opentelemetry` / `opentelemetry_sdk` | 0.31 | OTel SDK |
| `opentelemetry-otlp` | 0.31 | OTLP HTTP exporter |
| `tracing` / `tracing-subscriber` | 0.1 / 0.3 | Structured logging |
| `tracing-opentelemetry` | 0.32 | Bridge tracing spans → OTel |
| `tracing-actix-web` | 0.7 | HTTP request tracing middleware |
| `utoipa` | 5.4 | OpenAPI schema generation from annotations |
| `linkme` | 0.3.33 | Distributed static slices (task registry) |
| `rustyline` | 17.0.2 | Readline for CLI client |
| `ollama-rs` | 0.3.2 | Ollama LLM client for `sbc ask` command |
| `serde` / `serde_json` | 1.0 | Serialisation |
| `uuid` | 1.2.2 | UUID generation and parsing |
| `chrono` | 0.4 | Date/time |
| `anyhow` | 1.0 | Error handling |
| `reqwest` | 0.13.2 | HTTP client |
| `compile-time` | 0.2 | Embed Rust version/build time in binary |

### Node.js Packages

| Package | Version | Role |
|---------|---------|------|
| `isolated-vm` | ^6.0.0 | V8 isolate sandbox for user JS scripts |
| `ws` | ^8.18.2 | WebSocket server |

---

## Building & Running

### Quick start (Docker Compose — development)

```bash
# Copy and edit environment
cp .env .env.local
# edit .env.local if needed

# Start the full development stack
docker compose up -d

# To include the compiled Rust server:
docker compose --profile full up -d
```

Services started:
- PostgreSQL on `:5432`
- Mosquitto on `:1883` (TCP) and `:9001` (WS)
- Data transform service on `:9002`
- SBMI on `:8082`
- OTel Collector on `:4318`

### Build from source (Rust)

```bash
# All Rust crates are resolved offline if .sqlx/ is present
SQLX_OFFLINE=true cargo build --release

# Or for development (faster builds, debug info)
SQLX_OFFLINE=true cargo build
```

### Install Node.js service dependencies

```bash
cd services/node.js
npm install
```

### Run the main server locally

```bash
# Ensure PostgreSQL and Mosquitto are running (e.g. via docker compose up sb-postgres sb-service-mosquitto)
PSQL_USER=sensbee PSQL_PASSWORD=supersecretpw PSQL_DATABASE=sensbee_db \
  cargo run -p sb_srv
```

### Run the CLI client

```bash
cargo run -p sbc -- --server http://localhost:8080 --user admin@example.com
```

---

## Running Tests

Tests use `sqlx::test` which spins up an isolated PostgreSQL database per test. A running PostgreSQL instance is required.

```bash
# Set the database URL
export DATABASE_URL=postgres://sensbee:supersecretpw@localhost:5432/sensbee_db

# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p sensor_mgmt

# MQTT tests also require a running Mosquitto broker
# Start both services first:
docker compose up -d sb-postgres sb-service-mosquitto
cargo test
```

The `.sqlx/` offline query cache allows compilation without a database (`SQLX_OFFLINE=true`), but tests themselves need a live database with the migration applied.

---

## Contributing

SensBee is an open platform. Contributions to functionality, documentation, and testing are welcome.

1. Fork the repository.
2. Create a feature branch.
3. Ensure all existing tests pass (`cargo test`).
4. Open a merge/pull request with a clear description.

---

## License

SensBee is available under the MIT License. See [LICENSE](LICENSE) for details.

# GoSuite

A curated collection of modular backend utilities, task concurrency engines, and queue orchestrators for Go.

Designed to be **framework-agnostic** with **minimal external dependencies**—usable across standard Go services, web applications, background daemons, or standalone CLI workers.

> [!NOTE]
> This suite is under active development (**Beta / Alpha**). While core components originate from real-world usage, APIs and driver contracts are still stabilizing as feedback and test coverage expand.

---

## 🚀 Key Highlights

- **Multi-Module Monorepo**: Each package lives in its own directory with an isolated `go.mod`. You only download the dependencies required for the specific drivers you use.
- **Lean Core & Isolated Drivers**: Core engines keep external dependencies to a bare minimum, while broker adapters only pull their official, dedicated client SDKs.
- **Framework-Agnostic**: No hard coupling to any specific web framework. Works seamlessly with standard Go (`net/http`), any router, or headless background workers.
- **Transparent Maturity**: Explicitly tracks stability per package (`Beta` / `Alpha`) so you know which components are ready for staging tests and which are still evolving.
- **Type-Safe & Predictable**: Focused configuration structs, sensible defaults, and memory-conscious dispatch paths verified by benchmarks.

---

## 🚦 Package Maturity & Stability Status

Because `gosuite` was extracted from real-world workloads into a modular open-source suite, stability stages are explicitly communicated:

- **`🧪 Beta`**: Core architecture, APIs, unit tests, and benchmarks are complete and verified against real workloads. Ready for staging and production testing.
- **`🌱 Alpha`**: Driver implementation and functional tests are complete, but awaiting extended high-concurrency production verification and stress testing.

| Package | Module Import Path | Maturity Status | Description |
| :--- | :--- | :---: | :--- |
| [worker](v3/worker) | `github.com/rachmanzz/fiber-extras/v3/worker` | `🧪 Beta (v0.2.0)` | Resilient background worker engine with parallel sub-worker fan-out, panic safety & clean lifecycle hooks. |
| [queue](v3/queue) | `github.com/rachmanzz/fiber-extras/v3/queue` | `🧪 Beta (v0.2.0)` | Multi-driver asynchronous queue orchestrator with priority ordering, group fanout, and retry/DLQ. |
| [queue-redis](v3/queue-redis) | `github.com/rachmanzz/fiber-extras/v3/queue-redis` | `🧪 Beta (v0.2.1)` | Redis adapter for `v3/queue` with priority ZSet and delayed scheduling. |
| [queue-nats](v3/queue-nats) | `github.com/rachmanzz/fiber-extras/v3/queue-nats` | `🌱 Alpha (v0.2.1)` | NATS JetStream WorkQueue adapter for `v3/queue` with deduplication and prioritized pull. |
| [queue-rabbitmq](v3/queue-rabbitmq) | `github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq` | `🌱 Alpha (v0.2.1)` | AMQP 0.9.1 RabbitMQ adapter for `v3/queue` with x-max-priority and manual acknowledgments. |
| [queue-postgres](v3/queue-postgres) | `github.com/rachmanzz/fiber-extras/v3/queue-postgres` | `🌱 Alpha (v0.2.0)` | Transactional PostgreSQL adapter for `v3/queue` using SELECT FOR UPDATE SKIP LOCKED. |
| [queue-msgpack](v3/queue-msgpack) | `github.com/rachmanzz/fiber-extras/v3/queue-msgpack` | `🌱 Alpha (v0.2.0)` | Pluggable MessagePack payload codec for `v3/queue` (`queue.WithCodec`), working across every adapter. |
| [cron](v3/cron) | `github.com/rachmanzz/fiber-extras/v3/cron` | `🧪 Beta (v0.2.0)` | Production-grade scheduler with a Task contract, fluent schedule builders, panic recovery, skip-if-still-running, and graceful shutdown. |

---

## 📊 Performance Benchmarks

All benchmark results are measured using Go's built-in testing tool (`go test -bench=. -benchmem`) on Linux `amd64` (11th Gen Intel® Core™ i7-1165G7 @ 2.80GHz):

### Worker Engine (`v3/worker`)
| Benchmark | Operations | Latency | Memory / Op | Allocs / Op |
| :--- | :---: | :---: | :---: | :---: |
| `BenchmarkWorker_Dispatch_Async` | ~18M | **~200 ns/op** | 78 B/op | **1 allocs/op** |
| `BenchmarkWorker_ExecuteSync` | ~20M | **186 ns/op** | 320 B/op | **4 allocs/op** |
| `BenchmarkWorker_SubWorker_FanOut` | ~610k | **3,990 ns/op** | 680 B/op | **18 allocs/op** |

### Queue Engine (`v3/queue`)
| Benchmark | Operations | Latency | Memory / Op | Allocs / Op |
| :--- | :---: | :---: | :---: | :---: |
| `BenchmarkQueue_Memory_EnqueueDequeue` | ~17M | **138 ns/op** | 56 B/op | **3 allocs/op** |
| `BenchmarkQueue_Message_Creation` | ~6.8M | **345 ns/op** | 288 B/op | **4 allocs/op** |
| `BenchmarkQueue_Dispatch_JSON` | ~2.7M | **985 ns/op** | 378 B/op | **5 allocs/op** |

---

## 🛠️ Local Development

This repository uses Go Workspaces (`go.work`) for seamless multi-module development across local packages.

### Clone the repository

```bash
git clone https://github.com/rachmanzz/gosuite.git
cd gosuite
```

### Initialize or synchronize workspace

```bash
go work init
# Add submodules to workspace (e.g. go work use ./<package-name>)
```

### Running Tests

To run tests across all modules in the workspace:

```bash
# Run tests for a specific module
go test -v ./v3/worker/...

# Or run tests directly within any submodule directory
cd v3/worker && go test -v ./...
```

### Integration Tests (live brokers)

The broker adapters (`queue-redis`, `queue-postgres`, `queue-nats`, `queue-rabbitmq`) ship live integration tests that exercise real brokers. They are gated by environment variables: when the matching variable is unset the test is skipped — except `queue-nats`, which falls back to an embedded in-process JetStream server.

| Adapter | Environment variable |
| :--- | :--- |
| `v3/queue-redis` | `TEST_REDIS_ADDR` — e.g. `127.0.0.1:6379` |
| `v3/queue-postgres` | `TEST_POSTGRES_DSN` — e.g. `postgres://fiber:secretpassword@127.0.0.1:5432/fiber_test?sslmode=disable` |
| `v3/queue-nats` | `TEST_NATS_URL` — e.g. `nats://127.0.0.1:4222` (embedded server when unset) |
| `v3/queue-rabbitmq` | `TEST_RABBITMQ_URL` — e.g. `amqp://fiber:secretpassword@127.0.0.1:5672/` |

The fastest way to run the whole suite is the one-shot harness. It boots every broker in containers with healthchecks, runs the adapter tests with the race detector, and tears everything down afterwards (zero residue):

```bash
./scripts/test-all-brokers.sh
```

It requires one of `docker compose`, `podman-compose`, or `docker-compose`.

To run a single adapter against a broker you already have:

```bash
cd v3/queue-redis
TEST_REDIS_ADDR=127.0.0.1:6379 go test -v ./...
```

Each adapter also has a dedicated CI workflow under `.github/workflows/` that provisions its broker. Redis, PostgreSQL and RabbitMQ use GitHub `services:` containers; NATS uses an explicit `docker run` because JetStream requires command flags (`-js -m 8222`) that `services:` cannot pass.

---

## 🤝 Contributing

Contributions are warmly welcome! Whether you are proposing a new module, queue driver, performance improvement, or bug fix, feel free to open an issue or submit a pull request.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

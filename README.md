# Fiber Extras

A curated collection of modular extensions, middlewares, and utility packages for [Fiber v3](https://github.com/gofiber/fiber), inspired by [gofiber/contrib](https://github.com/gofiber/contrib).

Designed from the ground up for high versatility: **fully compatible with pure Fiber v3 applications**, while offering **first-class adapters and specialized helpers for [fiber-starter](https://github.com/rachmanzz/fiber-starter)**.

---

## 🚀 Key Highlights

- **Multi-Module Monorepo (Zero Bloat)**: Each extension lives in its own directory with a dedicated `go.mod`. You only import the dependencies your project actually uses.
- **Dual Compatibility**:
  - **Standard Fiber v3**: Idiomatic, zero-lock-in middleware and handlers adhering to core Fiber v3 standards.
  - **Fiber-Starter Optimized**: Dedicated adapter functions and configs tailored to `fiber-starter` conventions (standardized JSON/MsgPack responses, centralized error handlers, and structured logging).
- **Efficient & Type-Safe**: Minimal heap allocations on hot dispatch paths, clean configuration structs, and sensible defaults backed by automated benchmarks.

---

## 🚦 Package Maturity & Stability Status

Because `fiber-extras` was extracted from real-world production workloads into a standalone open-source library, stability stages are explicitly communicated:

- **`🧪 Beta (v0.1.0)`**: Core architecture, APIs, unit tests, and benchmarks are complete and verified against real workloads. Ready for staging and production testing.
- **`🌱 Alpha (v0.1.0)`**: Driver implementation and functional tests are complete, but awaiting extended high-concurrency production verification and stress testing.

| Package | Module Import Path | Maturity Status | Description |
| :--- | :--- | :---: | :--- |
| [worker](v3/worker) | `github.com/rachmanzz/fiber-extras/v3/worker` | `🧪 Beta (v0.1.0)` | Resilient background worker engine with parallel sub-worker fan-out, panic safety & clean lifecycle hooks. |
| [queue](v3/queue) | `github.com/rachmanzz/fiber-extras/v3/queue` | `🧪 Beta (v0.1.0)` | Multi-driver asynchronous queue orchestrator with priority ordering, group fanout, and retry/DLQ. |
| [queue-redis](v3/queue-redis) | `github.com/rachmanzz/fiber-extras/v3/queue-redis` | `🧪 Beta (v0.1.0)` | Redis adapter for `v3/queue` with priority ZSet and delayed scheduling. |
| [queue-nats](v3/queue-nats) | `github.com/rachmanzz/fiber-extras/v3/queue-nats` | `🌱 Alpha (v0.1.0)` | NATS JetStream WorkQueue adapter for `v3/queue` with deduplication and prioritized pull. |
| [queue-rabbitmq](v3/queue-rabbitmq) | `github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq` | `🌱 Alpha (v0.1.0)` | AMQP 0.9.1 RabbitMQ adapter for `v3/queue` with x-max-priority and manual acknowledgments. |
| [queue-postgres](v3/queue-postgres) | `github.com/rachmanzz/fiber-extras/v3/queue-postgres` | `🌱 Alpha (v0.1.0)` | Transactional PostgreSQL adapter for `v3/queue` using SELECT FOR UPDATE SKIP LOCKED. |
| [queue-msgpack](v3/queue-msgpack) | `github.com/rachmanzz/fiber-extras/v3/queue-msgpack` | `🌱 Alpha (v0.1.0)` | Pluggable MessagePack payload codec for `v3/queue` (`queue.WithCodec`), working across every adapter. |

---

## 📊 Performance Benchmarks

All benchmark results are measured using Go's built-in testing tool (`go test -bench=. -benchmem`) on Linux `amd64` (11th Gen Intel® Core™ i7-1165G7 @ 2.80GHz):

### Worker Engine (`v3/worker`)
| Benchmark | Operations | Latency | Memory / Op | Allocs / Op |
| :--- | :---: | :---: | :---: | :---: |
| `BenchmarkWorker_Dispatch_Async` | ~6.5M | **162.2 ns/op** | 88 B/op | **1 allocs/op** |
| `BenchmarkWorker_ExecuteSync` | ~4.9M | **311.3 ns/op** | 320 B/op | **4 allocs/op** |
| `BenchmarkWorker_SubWorker_FanOut` | ~318k | **3,970 ns/op** | 680 B/op | **18 allocs/op** |

### Queue Engine (`v3/queue`)
| Benchmark | Operations | Latency | Memory / Op | Allocs / Op |
| :--- | :---: | :---: | :---: | :---: |
| `BenchmarkQueue_Memory_EnqueueDequeue` | ~5.1M | **211.9 ns/op** | 80 B/op | **4 allocs/op** |
| `BenchmarkQueue_Message_Creation` | ~1.5M | **780.2 ns/op** | 392 B/op | **9 allocs/op** |
| `BenchmarkQueue_Dispatch_JSON` | ~20k | **117.7 µs/op** | 528 B/op | **12 allocs/op** |

---

## 🛠️ Local Development

This repository uses Go Workspaces (`go.work`) for seamless multi-module development across local packages.

### Clone the repository

```bash
git clone https://github.com/rachmanzz/fiber-extras.git
cd fiber-extras
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

Contributions are warmly welcome! Whether you are proposing a new Fiber v3 extension or adding adapters for `fiber-starter`, feel free to open an issue or submit a pull request.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

# Fiber Extras

A curated collection of modular extensions, middlewares, and utility packages for [Fiber v3](https://github.com/gofiber/fiber), inspired by [gofiber/contrib](https://github.com/gofiber/contrib).

Designed from the ground up for high versatility: **fully compatible with pure Fiber v3 applications**, while offering **first-class adapters and specialized helpers for [fiber-starter](https://github.com/rachmanzz/fiber-starter)**.

---

## 🚀 Key Highlights

- **Multi-Module Monorepo (Zero Bloat)**: Each extension lives in its own directory with a dedicated `go.mod`. You only import the dependencies your project actually uses.
- **Dual Compatibility**:
  - **Standard Fiber v3**: Idiomatic, zero-lock-in middleware and handlers adhering to core Fiber v3 standards.
  - **Fiber-Starter Optimized**: Dedicated adapter functions and configs tailored to `fiber-starter` conventions (standardized JSON/MsgPack responses, centralized error handlers, and structured logging).
- **High Performance & Type-Safe**: Minimal memory allocations, clean configuration structs, and robust defaults.

---

## 📂 Repository Architecture

This repository adopts the **multi-module layout** (similar to `gofiber/contrib`). Each package is an independent Go module, isolated from one another:

```text
fiber-extras/
├── go.work                    # Go workspace for local multi-module development
├── README.md                  # Root documentation
├── <package-a>/
│   ├── go.mod                 # Isolated module definition
│   ├── go.sum
│   ├── <package-a>.go         # Core implementation (Pure Fiber v3)
│   ├── <package-a>_test.go
│   ├── starter/               # (Optional) Fiber-starter specific adapter
│   └── README.md              # Package documentation
└── <package-b>/
    ├── go.mod
    └── ...
```

---

## 📦 Installation

To install any package from `fiber-extras`, target the specific module path:

```bash
go get github.com/rachmanzz/fiber-extras/<package-name>
```

> **Note:** Because each package is its own module, you only download dependencies specific to that package without pulling the rest of the repository.

---

## 💡 Usage Paradigms

Packages in `fiber-extras` provide two usage modes:

### 1. Pure Fiber v3 (Standard / Global)

Use the package directly with standard Fiber v3 instances:

```go
package main

import (
    "github.com/gofiber/fiber/v3"
    // Example: importing a hypothetical extras package
    // "github.com/rachmanzz/fiber-extras/<package-name>"
)

func main() {
    app := fiber.New()

    // Standard middleware / utility usage
    // app.Use(<package-name>.New())

    app.Listen(":3000")
}
```

### 2. Fiber-Starter Integration

When building with [fiber-starter](https://github.com/rachmanzz/fiber-starter), use starter-optimized constructors or adapters to automatically align with its response format, logging, and error handling pipeline:

```go
package main

import (
    // Example: importing starter adapter
    // "github.com/rachmanzz/fiber-extras/<package-name>/starter"
    // or using starter helper constructor: <package-name>.NewForStarter(...)
)

// Automatically integrates with fiber-starter's cores.AppContracts,
// BaseResponse formatting, and custom error mappers.
```

---

## 🗺️ Packages Directory

| Package | Status | Description |
| :--- | :---: | :--- |
| *Planning / In-progress* | 🚧 | Modular packages will be listed here as they are added. |

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
go test ./...
```

---

## 🤝 Contributing

Contributions are warmly welcome! Whether you are proposing a new Fiber v3 extension or adding adapters for `fiber-starter`, feel free to open an issue or submit a pull request.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

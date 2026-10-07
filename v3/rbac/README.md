# RBAC Middleware for Fiber v3

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/rbac.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/rbac)

RBAC (Role-Based Access Control) middleware and route-level guards for [Fiber v3](https://github.com/gofiber/fiber), powered by [rbacgo](https://github.com/rachmanzz/rbacgo).

Provides zero-overhead, plug-and-play authorization for **pure Fiber v3** applications while featuring out-of-the-box adapters for **[fiber-starter](https://github.com/rachmanzz/fiber-starter)** conventions.

---

## 📦 Installation

```bash
go get github.com/rachmanzz/fiber-extras/v3/rbac
```

---

## 🚀 Usage

### 1. Pure Fiber v3 (Standard)

#### Basic Setup

```go
package main

import (
    "github.com/gofiber/fiber/v3"
    "github.com/rachmanzz/fiber-extras/v3/rbac"
    "github.com/rachmanzz/rbacgo"
)

func main() {
    app := fiber.New()

    // 1. Initialize rbacgo enforcer
    enforcer, _ := rbacgo.New(
        rbacgo.WithTenant("default"),
        rbacgo.WithMemoryStore(), // or WithSQLite / WithSQLStore
    )

    // 2. Global RBAC middleware (matches request Path and Method)
    app.Use(rbac.New(rbac.Config{
        Enforcer: enforcer,
    }))

    // 3. Fine-grained route guards
    app.Get("/articles", rbac.Require("/articles", "GET", rbac.Config{Enforcer: enforcer}), listArticles)
    app.Post("/articles", rbac.Require("articles", "publish", rbac.Config{Enforcer: enforcer}), publishArticle)

    // 4. Role guards
    app.Get("/admin", rbac.RequireRole("admin", rbac.Config{Enforcer: enforcer}), adminDashboard)
    app.Get("/team", rbac.RequireAnyRole([]string{"editor", "manager"}, rbac.Config{Enforcer: enforcer}), teamDashboard)

    app.Listen(":3000")
}
```

#### Dynamic / Parameterized Route Guard

Extract dynamic resource or action parameters from route variables:

```go
app.Get("/projects/:id", rbac.RequireDynamic(func(c fiber.Ctx) (string, string) {
    projectID := c.Params("id")
    return "projects:" + projectID, "read"
}, rbac.Config{Enforcer: enforcer}), getProject)
```

---

### 2. Multi-Tenant RBAC

Easily manage isolated RBAC policies per tenant with lazy initialization and concurrent caching:

```go
registry := rbac.NewTenantRegistry(func(tenantID string) (*rbacgo.Enforcer, error) {
    return rbacgo.New(
        rbacgo.WithTenant(tenantID),
        rbacgo.WithSQLStore(db),
    )
})

app.Use(rbac.NewTenant(rbac.TenantConfig{
    Registry: registry,
    // By default reads "X-Tenant-ID" header or c.Locals("tenantId")
}))
```

---

### 3. Integration with `fiber-starter`

When using [fiber-starter](https://github.com/rachmanzz/fiber-starter), use dedicated starter helpers. They automatically:
- Extract subject identity from `c.Locals("userID")`.
- Format 401 and 403 errors using `cores.BaseResponse` (`{ "success": false, "message": "..." }`).
- Negotiate between **JSON** and **MessagePack** (`Accept: application/x-msgpack`).

```go
package main

import (
    "github.com/gofiber/fiber/v3"
    "github.com/rachmanzz/fiber-extras/v3/rbac"
    "github.com/rachmanzz/rbacgo"
)

func RegisterRBAC(app *fiber.App, enforcer *rbacgo.Enforcer) {
    // Global middleware for starter
    app.Use(rbac.NewForStarter(enforcer))

    // Route guards formatted for starter
    app.Get("/reports", rbac.RequireForStarter(enforcer, "reports", "read"), handler)
    app.Post("/reports", rbac.RequireRoleForStarter(enforcer, "manager"), handler)
}
```

---

## ⚙️ Configuration Reference

### `rbac.Config`

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `Next` | `func(fiber.Ctx) bool` | `nil` | Function to skip this middleware when returning true. |
| `Enforcer` | `*rbacgo.Enforcer` | `nil` (Required) | The `rbacgo.Enforcer` instance. |
| `Subject` | `func(fiber.Ctx) (string, error)` | Reads `userId`, `user_id`, or `sub` from `c.Locals` | Function extracting subject/user ID. |
| `ResourceAction` | `func(fiber.Ctx) (string, string)` | `(c.Path(), c.Method())` | Function deriving resource and action from request. |
| `Unauthorized` | `fiber.Handler` | `fiber.ErrUnauthorized` | Handler invoked when subject is unauthenticated. |
| `Forbidden` | `fiber.Handler` | `fiber.ErrForbidden` | Handler invoked when permission is denied. |

---

## 📄 License

MIT

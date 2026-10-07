package rbac_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/rachmanzz/fiber-extras/v3/rbac"
	"github.com/rachmanzz/rbacgo"
)

func setupTestEnforcer(t *testing.T) *rbacgo.Enforcer {
	t.Helper()
	enf, err := rbacgo.New(
		rbacgo.WithTenant("default-tenant"),
		rbacgo.WithMemoryStore(),
	)
	if err != nil {
		t.Fatalf("failed to create enforcer: %v", err)
	}

	ctx := context.Background()
	roles := []rbacgo.Role{
		{
			Name: "editor",
			Permissions: []rbacgo.Permission{
				{Resource: "/articles", Action: "GET"},
				{Resource: "/articles", Action: "POST"},
				{Resource: "articles", Action: "publish"},
			},
		},
		{
			Name: "viewer",
			Permissions: []rbacgo.Permission{
				{Resource: "/articles", Action: "GET"},
			},
		},
		{
			Name: "admin",
		},
	}
	if err := enf.RegisterRoles(ctx, roles...); err != nil {
		t.Fatalf("failed to register roles: %v", err)
	}

	_ = enf.AssignRole(ctx, "alice", "editor")
	_ = enf.AssignRole(ctx, "alice", "admin")
	_ = enf.AssignRole(ctx, "bob", "viewer")

	return enf
}

func TestNew_Middleware(t *testing.T) {
	enf := setupTestEnforcer(t)

	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if u := c.Get("X-User-ID"); u != "" {
			c.Locals("userId", u)
		}
		return c.Next()
	})

	app.Use(rbac.New(rbac.Config{
		Enforcer: enf,
	}))

	app.Get("/articles", func(c fiber.Ctx) error {
		subject := rbac.SubjectFromContext(c)
		return c.SendString("allowed for " + subject)
	})
	app.Post("/articles", func(c fiber.Ctx) error {
		return c.SendString("created")
	})

	t.Run("unauthenticated returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/articles", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}
	})

	t.Run("bob can GET /articles", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/articles", nil)
		req.Header.Set("X-User-ID", "bob")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "allowed for bob" {
			t.Fatalf("unexpected body: %s", string(body))
		}
	})

	t.Run("bob cannot POST /articles (403)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/articles", nil)
		req.Header.Set("X-User-ID", "bob")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusForbidden {
			t.Fatalf("expected 403, got %d", resp.StatusCode)
		}
	})

	t.Run("alice can POST /articles", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/articles", nil)
		req.Header.Set("X-User-ID", "alice")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})
}

func TestRequire_RouteGuard(t *testing.T) {
	enf := setupTestEnforcer(t)

	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if u := c.Get("X-User-ID"); u != "" {
			c.Locals("userId", u)
		}
		return c.Next()
	})

	app.Post("/publish", rbac.Require("articles", "publish", rbac.Config{
		Enforcer: enf,
	}), func(c fiber.Ctx) error {
		return c.SendString("published")
	})

	t.Run("unauthenticated (401)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/publish", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}
	})

	t.Run("bob forbidden (403)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/publish", nil)
		req.Header.Set("X-User-ID", "bob")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusForbidden {
			t.Fatalf("expected 403, got %d", resp.StatusCode)
		}
	})

	t.Run("alice allowed (200)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/publish", nil)
		req.Header.Set("X-User-ID", "alice")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})
}

func TestRequireRole(t *testing.T) {
	enf := setupTestEnforcer(t)

	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if u := c.Get("X-User-ID"); u != "" {
			c.Locals("user_id", u)
		}
		return c.Next()
	})

	app.Get("/admin", rbac.RequireRole("admin", rbac.Config{Enforcer: enf}), func(c fiber.Ctx) error {
		return c.SendString("admin panel")
	})

	app.Get("/team", rbac.RequireAnyRole([]string{"editor", "superadmin"}, rbac.Config{Enforcer: enf}), func(c fiber.Ctx) error {
		return c.SendString("team space")
	})

	app.Get("/root", rbac.RequireAllRoles([]string{"editor", "admin"}, rbac.Config{Enforcer: enf}), func(c fiber.Ctx) error {
		return c.SendString("root")
	})

	t.Run("bob denied admin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin", nil)
		req.Header.Set("X-User-ID", "bob")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusForbidden {
			t.Fatalf("expected 403, got %d", resp.StatusCode)
		}
	})

	t.Run("alice allowed admin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin", nil)
		req.Header.Set("X-User-ID", "alice")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("alice allowed team (has editor)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/team", nil)
		req.Header.Set("X-User-ID", "alice")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("alice allowed root (has both editor and admin)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/root", nil)
		req.Header.Set("X-User-ID", "alice")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})
}

func TestRequireDynamic(t *testing.T) {
	enf := setupTestEnforcer(t)

	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if u := c.Get("X-User-ID"); u != "" {
			c.Locals("sub", u)
		}
		return c.Next()
	})

	guard := rbac.RequireDynamic(func(c fiber.Ctx) (string, string) {
		res := c.Params("resource")
		act := c.Params("action")
		return res, act
	}, rbac.Config{Enforcer: enf})

	app.Get("/perm/:resource/:action", guard, func(c fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/perm/articles/publish", nil)
	req.Header.Set("X-User-ID", "alice")
	resp, _ := app.Test(req)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	req = httptest.NewRequest(http.MethodGet, "/perm/articles/publish", nil)
	req.Header.Set("X-User-ID", "bob")
	resp, _ = app.Test(req)
	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestNext_Skip(t *testing.T) {
	enf := setupTestEnforcer(t)

	app := fiber.New()
	app.Use(rbac.New(rbac.Config{
		Enforcer: enf,
		Next: func(c fiber.Ctx) bool {
			return c.Path() == "/health"
		},
	}))

	app.Get("/health", func(c fiber.Ctx) error {
		return c.SendString("healthy")
	})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	resp, _ := app.Test(req)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestPanicOnNilEnforcer(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil enforcer")
		}
	}()
	_ = rbac.New(rbac.Config{Enforcer: nil})
}

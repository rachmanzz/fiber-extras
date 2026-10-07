package rbac_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/rachmanzz/fiber-extras/v3/rbac"
	"github.com/rachmanzz/rbacgo"
)

func TestTenant_Middleware(t *testing.T) {
	registry := rbac.NewTenantRegistry(func(tenant string) (*rbacgo.Enforcer, error) {
		if tenant == "unknown" {
			return nil, errors.New("tenant not found")
		}
		enf, err := rbacgo.New(
			rbacgo.WithTenant(tenant),
			rbacgo.WithMemoryStore(),
		)
		if err != nil {
			return nil, err
		}
		ctx := context.Background()
		if tenant == "tenant-a" {
			_ = enf.RegisterRoles(ctx, rbacgo.Role{
				Name: "admin",
				Permissions: []rbacgo.Permission{
					{Resource: "/data", Action: "GET"},
				},
			})
			_ = enf.AssignRole(ctx, "user1", "admin")
		}
		return enf, nil
	})

	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if u := c.Get("X-User-ID"); u != "" {
			c.Locals("userId", u)
		}
		return c.Next()
	})

	app.Use(rbac.NewTenant(rbac.TenantConfig{
		Registry: registry,
	}))

	app.Get("/data", func(c fiber.Ctx) error {
		tenant := rbac.TenantFromContext(c)
		return c.SendString("data for " + tenant)
	})

	t.Run("missing tenant header returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/data", nil)
		req.Header.Set("X-User-ID", "user1")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}
	})

	t.Run("invalid tenant returns 500 error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/data", nil)
		req.Header.Set("X-Tenant-ID", "unknown")
		req.Header.Set("X-User-ID", "user1")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", resp.StatusCode)
		}
	})

	t.Run("tenant-a authorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/data", nil)
		req.Header.Set("X-Tenant-ID", "tenant-a")
		req.Header.Set("X-User-ID", "user1")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("tenant-b forbidden", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/data", nil)
		req.Header.Set("X-Tenant-ID", "tenant-b")
		req.Header.Set("X-User-ID", "user1")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusForbidden {
			t.Fatalf("expected 403, got %d", resp.StatusCode)
		}
	})
}

package rbac_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/rachmanzz/fiber-extras/v3/rbac"
	"github.com/rachmanzz/rbacgo"
)

func TestStarter_Adapter(t *testing.T) {
	enf, err := rbacgo.New(
		rbacgo.WithTenant("starter-tenant"),
		rbacgo.WithMemoryStore(),
	)
	if err != nil {
		t.Fatalf("failed to create enforcer: %v", err)
	}

	ctx := context.Background()
	_ = enf.RegisterRoles(ctx, rbacgo.Role{
		Name: "manager",
		Permissions: []rbacgo.Permission{
			{Resource: "/reports", Action: "GET"},
		},
	})
	_ = enf.AssignRole(ctx, "user-1", "manager")

	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if u := c.Get("X-User-ID"); u != "" {
			c.Locals("userID", u)
		}
		return c.Next()
	})

	app.Use(rbac.NewForStarter(enf))

	app.Get("/reports", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"data": "report-data"})
	})

	t.Run("unauthorized returns BaseResponse JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/reports", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		var res rbac.StarterResponse
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, string(body))
		}
		if res.Success || res.Message != "Unauthorized" {
			t.Fatalf("unexpected BaseResponse content: %+v", res)
		}
	})

	t.Run("forbidden returns BaseResponse JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/reports", nil)
		req.Header.Set("X-User-ID", "other-user")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusForbidden {
			t.Fatalf("expected 403, got %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		var res rbac.StarterResponse
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, string(body))
		}
		if res.Success || res.Message != "Forbidden" {
			t.Fatalf("unexpected BaseResponse content: %+v", res)
		}
	})

	t.Run("authorized succeeds", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/reports", nil)
		req.Header.Set("X-User-ID", "user-1")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})
}

func TestStarter_RouteGuards(t *testing.T) {
	enf, err := rbacgo.New(
		rbacgo.WithTenant("starter-tenant"),
		rbacgo.WithMemoryStore(),
	)
	if err != nil {
		t.Fatalf("failed to create enforcer: %v", err)
	}

	ctx := context.Background()
	_ = enf.RegisterRoles(ctx, rbacgo.Role{
		Name: "admin",
	})
	_ = enf.AssignRole(ctx, "admin-1", "admin")

	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if u := c.Get("X-User-ID"); u != "" {
			c.Locals("userID", u)
		}
		return c.Next()
	})

	app.Post("/metrics", rbac.RequireRoleForStarter(enf, "admin"), func(c fiber.Ctx) error {
		return c.SendString("ok")
	})

	t.Run("denied role returns BaseResponse", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
		req.Header.Set("X-User-ID", "guest")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusForbidden {
			t.Fatalf("expected 403, got %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		var res rbac.StarterResponse
		_ = json.Unmarshal(body, &res)
		if res.Success || res.Message != "Forbidden" {
			t.Fatalf("expected BaseResponse forbidden, got %+v", res)
		}
	})

	t.Run("allowed role passes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
		req.Header.Set("X-User-ID", "admin-1")
		resp, _ := app.Test(req)
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})
}

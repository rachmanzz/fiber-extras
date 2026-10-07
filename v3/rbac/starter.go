package rbac

import (
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/rachmanzz/rbacgo"
)

// StarterResponse represents the standardized JSON/MsgPack envelope used by fiber-starter.
type StarterResponse struct {
	Success bool   `json:"success" msgpack:"success"`
	Message string `json:"message" msgpack:"message"`
	Data    any    `json:"data,omitempty" msgpack:"data,omitempty"`
	Error   any    `json:"error,omitempty" msgpack:"error,omitempty"`
}

// SendStarterResponse writes a structured response matching fiber-starter conventions,
// with automatic Content-Type negotiation between JSON and MessagePack.
func SendStarterResponse(c fiber.Ctx, status int, payload StarterResponse) error {
	c.Vary(fiber.HeaderAccept)

	match := c.Accepts("application/json", "application/x-msgpack", "application/msgpack", "application/vnd.msgpack")
	if match != "" && match != "application/json" {
		if err := c.Status(status).MsgPack(payload, match); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Internal Server Error"})
		}
		return nil
	}

	return c.Status(status).JSON(payload)
}

// StarterUnauthorizedHandler returns a 401 response conforming to fiber-starter's BaseResponse structure.
func StarterUnauthorizedHandler(c fiber.Ctx) error {
	return SendStarterResponse(c, fiber.StatusUnauthorized, StarterResponse{
		Success: false,
		Message: "Unauthorized",
	})
}

// StarterForbiddenHandler returns a 403 response conforming to fiber-starter's BaseResponse structure.
func StarterForbiddenHandler(c fiber.Ctx) error {
	return SendStarterResponse(c, fiber.StatusForbidden, StarterResponse{
		Success: false,
		Message: "Forbidden",
	})
}

// StarterTenantErrorHandler returns a 500 response conforming to fiber-starter's BaseResponse structure.
func StarterTenantErrorHandler(c fiber.Ctx, err error) error {
	var errDetails any
	if err != nil {
		errDetails = err.Error()
	}
	return SendStarterResponse(c, fiber.StatusInternalServerError, StarterResponse{
		Success: false,
		Message: "Internal Server Error",
		Error:   errDetails,
	})
}

// StarterSubjectExtractor extracts the authenticated subject ID aligned with fiber-starter conventions.
// It checks c.Locals for "userID", "userId", "user_id", or "sub".
func StarterSubjectExtractor(c fiber.Ctx) (string, error) {
	keys := []string{"userID", "userId", "user_id", "sub", "id"}
	for _, k := range keys {
		if val := c.Locals(k); val != nil {
			if str, ok := val.(string); ok && strings.TrimSpace(str) != "" {
				return strings.TrimSpace(str), nil
			}
		}
	}
	return "", ErrSubjectNotFound
}

// ConfigForStarter creates a Config preconfigured with fiber-starter conventions.
func ConfigForStarter(enforcer *rbacgo.Enforcer, overrides ...Config) Config {
	cfg := Config{
		Enforcer:       enforcer,
		Subject:        StarterSubjectExtractor,
		ResourceAction: defaultResourceAction,
		Unauthorized:   StarterUnauthorizedHandler,
		Forbidden:      StarterForbiddenHandler,
	}

	if len(overrides) > 0 {
		ov := overrides[0]
		if ov.Next != nil {
			cfg.Next = ov.Next
		}
		if ov.Subject != nil {
			cfg.Subject = ov.Subject
		}
		if ov.ResourceAction != nil {
			cfg.ResourceAction = ov.ResourceAction
		}
		if ov.Unauthorized != nil {
			cfg.Unauthorized = ov.Unauthorized
		}
		if ov.Forbidden != nil {
			cfg.Forbidden = ov.Forbidden
		}
	}

	return cfg
}

// TenantConfigForStarter creates a TenantConfig preconfigured with fiber-starter conventions.
func TenantConfigForStarter(registry *TenantRegistry, overrides ...TenantConfig) TenantConfig {
	cfg := TenantConfig{
		Registry:       registry,
		Tenant:         defaultTenantExtractor,
		Subject:        StarterSubjectExtractor,
		ResourceAction: defaultResourceAction,
		Unauthorized:   StarterUnauthorizedHandler,
		Forbidden:      StarterForbiddenHandler,
		TenantError:    StarterTenantErrorHandler,
	}

	if len(overrides) > 0 {
		ov := overrides[0]
		if ov.Next != nil {
			cfg.Next = ov.Next
		}
		if ov.Tenant != nil {
			cfg.Tenant = ov.Tenant
		}
		if ov.Subject != nil {
			cfg.Subject = ov.Subject
		}
		if ov.ResourceAction != nil {
			cfg.ResourceAction = ov.ResourceAction
		}
		if ov.Unauthorized != nil {
			cfg.Unauthorized = ov.Unauthorized
		}
		if ov.Forbidden != nil {
			cfg.Forbidden = ov.Forbidden
		}
		if ov.TenantError != nil {
			cfg.TenantError = ov.TenantError
		}
	}

	return cfg
}

// NewForStarter creates an RBAC middleware pre-wired for fiber-starter.
func NewForStarter(enforcer *rbacgo.Enforcer, overrides ...Config) fiber.Handler {
	return New(ConfigForStarter(enforcer, overrides...))
}

// RequireForStarter creates a route guard pre-wired for fiber-starter.
func RequireForStarter(enforcer *rbacgo.Enforcer, resource, action string, overrides ...Config) fiber.Handler {
	return Require(resource, action, ConfigForStarter(enforcer, overrides...))
}

// RequireRoleForStarter creates a role guard pre-wired for fiber-starter.
func RequireRoleForStarter(enforcer *rbacgo.Enforcer, role string, overrides ...Config) fiber.Handler {
	return RequireRole(role, ConfigForStarter(enforcer, overrides...))
}

// RequireTenantForStarter creates a multi-tenant route guard pre-wired for fiber-starter.
func RequireTenantForStarter(registry *TenantRegistry, resource, action string, overrides ...TenantConfig) fiber.Handler {
	return RequireTenant(resource, action, TenantConfigForStarter(registry, overrides...))
}

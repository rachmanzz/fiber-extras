// Package rbac provides an RBAC middleware for Fiber v3 powered by rbacgo.
//
// It supports both pure Fiber v3 applications and first-class integrations
// for fiber-starter boilerplates.
package rbac

import (
	"context"
	"errors"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/rachmanzz/rbacgo"
)

type contextKey string

const (
	enforcerKey contextKey = "rbac_enforcer"
	subjectKey  contextKey = "rbac_subject"
)

// Common errors.
var (
	ErrNilEnforcer       = errors.New("rbac: enforcer must not be nil")
	ErrSubjectNotFound   = errors.New("rbac: subject identity not found in request")
	ErrSubjectEmpty      = errors.New("rbac: subject identity is empty")
	ErrTenantNotFound    = errors.New("rbac: tenant identity not found in request")
	ErrNilTenantRegistry = errors.New("rbac: tenant registry must not be nil")
)

// Config defines the configuration options for the RBAC middleware.
type Config struct {
	// Next defines a function to skip this middleware when returning true.
	// Optional. Default: nil
	Next func(c fiber.Ctx) bool

	// Enforcer is the rbacgo.Enforcer instance.
	// Required for global or route middleware (unless using multi-tenant).
	Enforcer *rbacgo.Enforcer

	// Subject extracts the authenticated subject ID (e.g. user ID) from the request context.
	// Optional. Default: extracts from c.Locals ("userId", "user_id", or "sub").
	Subject func(c fiber.Ctx) (string, error)

	// ResourceAction extracts the resource and action pair from the request.
	// Optional. Default: (c.Path(), c.Method()).
	ResourceAction func(c fiber.Ctx) (string, string)

	// Unauthorized is executed when the subject cannot be extracted or is empty.
	// Optional. Default: returns fiber.ErrUnauthorized.
	Unauthorized fiber.Handler

	// Forbidden is executed when the RBAC enforcement check fails (403).
	// Optional. Default: returns fiber.ErrForbidden.
	Forbidden fiber.Handler
}

// ConfigDefault provides the default configuration for RBAC middleware.
var ConfigDefault = Config{
	Subject:        defaultSubjectExtractor,
	ResourceAction: defaultResourceAction,
	Unauthorized:   defaultUnauthorizedHandler,
	Forbidden:      defaultForbiddenHandler,
}

func defaultSubjectExtractor(c fiber.Ctx) (string, error) {
	candidates := []string{"userId", "user_id", "sub", "subject"}
	for _, key := range candidates {
		if val := c.Locals(key); val != nil {
			if strVal, ok := val.(string); ok && strings.TrimSpace(strVal) != "" {
				return strings.TrimSpace(strVal), nil
			}
		}
	}
	return "", ErrSubjectNotFound
}

func defaultResourceAction(c fiber.Ctx) (string, string) {
	return c.Path(), c.Method()
}

func defaultUnauthorizedHandler(c fiber.Ctx) error {
	return fiber.ErrUnauthorized
}

func defaultForbiddenHandler(c fiber.Ctx) error {
	return fiber.ErrForbidden
}

func configDefault(cfg ...Config) Config {
	if len(cfg) == 0 {
		return ConfigDefault
	}

	c := cfg[0]
	if c.Subject == nil {
		c.Subject = ConfigDefault.Subject
	}
	if c.ResourceAction == nil {
		c.ResourceAction = ConfigDefault.ResourceAction
	}
	if c.Unauthorized == nil {
		c.Unauthorized = ConfigDefault.Unauthorized
	}
	if c.Forbidden == nil {
		c.Forbidden = ConfigDefault.Forbidden
	}

	return c
}

// New creates an RBAC middleware handler for Fiber v3.
// It enforces permissions automatically by matching (c.Path(), c.Method())
// or the custom ResourceAction extractor against the provided Enforcer.
func New(config ...Config) fiber.Handler {
	cfg := configDefault(config...)

	if cfg.Enforcer == nil {
		panic(ErrNilEnforcer)
	}

	return func(c fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		subject, err := cfg.Subject(c)
		if err != nil || strings.TrimSpace(subject) == "" {
			return cfg.Unauthorized(c)
		}

		// Store enforcer and subject in locals and request context
		c.Locals(string(subjectKey), subject)
		c.Locals(string(enforcerKey), cfg.Enforcer)
		setContextValues(c, subject, cfg.Enforcer)

		resource, action := cfg.ResourceAction(c)
		if !cfg.Enforcer.Enforce(c.Context(), subject, resource, action) {
			return cfg.Forbidden(c)
		}

		return c.Next()
	}
}

// Require creates a route-level guard that enforces a specific (resource, action) permission.
func Require(resource, action string, config ...Config) fiber.Handler {
	cfg := configDefault(config...)

	if cfg.Enforcer == nil {
		panic(ErrNilEnforcer)
	}

	return func(c fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		subject, err := cfg.Subject(c)
		if err != nil || strings.TrimSpace(subject) == "" {
			return cfg.Unauthorized(c)
		}

		c.Locals(string(subjectKey), subject)
		c.Locals(string(enforcerKey), cfg.Enforcer)
		setContextValues(c, subject, cfg.Enforcer)

		if !cfg.Enforcer.Enforce(c.Context(), subject, resource, action) {
			return cfg.Forbidden(c)
		}

		return c.Next()
	}
}

// RequireRole creates a route-level guard that checks if the subject has a specific role.
func RequireRole(role string, config ...Config) fiber.Handler {
	cfg := configDefault(config...)

	if cfg.Enforcer == nil {
		panic(ErrNilEnforcer)
	}

	return func(c fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		subject, err := cfg.Subject(c)
		if err != nil || strings.TrimSpace(subject) == "" {
			return cfg.Unauthorized(c)
		}

		c.Locals(string(subjectKey), subject)
		c.Locals(string(enforcerKey), cfg.Enforcer)
		setContextValues(c, subject, cfg.Enforcer)

		hasRole, err := cfg.Enforcer.HasRole(c.Context(), subject, role)
		if err != nil || !hasRole {
			return cfg.Forbidden(c)
		}

		return c.Next()
	}
}

// RequireAnyRole creates a route-level guard that checks if the subject has AT LEAST ONE of the given roles.
func RequireAnyRole(roles []string, config ...Config) fiber.Handler {
	cfg := configDefault(config...)

	if cfg.Enforcer == nil {
		panic(ErrNilEnforcer)
	}

	return func(c fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		subject, err := cfg.Subject(c)
		if err != nil || strings.TrimSpace(subject) == "" {
			return cfg.Unauthorized(c)
		}

		c.Locals(string(subjectKey), subject)
		c.Locals(string(enforcerKey), cfg.Enforcer)
		setContextValues(c, subject, cfg.Enforcer)

		hasAny := false
		for _, role := range roles {
			if ok, err := cfg.Enforcer.HasRole(c.Context(), subject, role); err == nil && ok {
				hasAny = true
				break
			}
		}

		if !hasAny {
			return cfg.Forbidden(c)
		}

		return c.Next()
	}
}

// RequireAllRoles creates a route-level guard that checks if the subject has ALL of the specified roles.
func RequireAllRoles(roles []string, config ...Config) fiber.Handler {
	cfg := configDefault(config...)

	if cfg.Enforcer == nil {
		panic(ErrNilEnforcer)
	}

	return func(c fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		subject, err := cfg.Subject(c)
		if err != nil || strings.TrimSpace(subject) == "" {
			return cfg.Unauthorized(c)
		}

		c.Locals(string(subjectKey), subject)
		c.Locals(string(enforcerKey), cfg.Enforcer)
		setContextValues(c, subject, cfg.Enforcer)

		for _, role := range roles {
			ok, err := cfg.Enforcer.HasRole(c.Context(), subject, role)
			if err != nil || !ok {
				return cfg.Forbidden(c)
			}
		}

		return c.Next()
	}
}

// RequireDynamic creates a route-level guard where resource and action are computed dynamically
// from the request context (e.g. from URL route params or payload).
func RequireDynamic(resolver func(c fiber.Ctx) (string, string), config ...Config) fiber.Handler {
	cfg := configDefault(config...)

	if cfg.Enforcer == nil {
		panic(ErrNilEnforcer)
	}
	if resolver == nil {
		resolver = cfg.ResourceAction
	}

	return func(c fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		subject, err := cfg.Subject(c)
		if err != nil || strings.TrimSpace(subject) == "" {
			return cfg.Unauthorized(c)
		}

		c.Locals(string(subjectKey), subject)
		c.Locals(string(enforcerKey), cfg.Enforcer)
		setContextValues(c, subject, cfg.Enforcer)

		resource, action := resolver(c)
		if !cfg.Enforcer.Enforce(c.Context(), subject, resource, action) {
			return cfg.Forbidden(c)
		}

		return c.Next()
	}
}

// SubjectFromContext extracts the subject identity stored by the RBAC middleware.
func SubjectFromContext(c fiber.Ctx) string {
	if val, ok := c.Locals(string(subjectKey)).(string); ok && val != "" {
		return val
	}
	if val, ok := c.Context().Value(subjectKey).(string); ok && val != "" {
		return val
	}
	return ""
}

// EnforcerFromContext retrieves the active *rbacgo.Enforcer from the request context.
func EnforcerFromContext(c fiber.Ctx) *rbacgo.Enforcer {
	if enf, ok := c.Locals(string(enforcerKey)).(*rbacgo.Enforcer); ok && enf != nil {
		return enf
	}
	if enf, ok := c.Context().Value(enforcerKey).(*rbacgo.Enforcer); ok && enf != nil {
		return enf
	}
	return nil
}

func setContextValues(c fiber.Ctx, subject string, enforcer *rbacgo.Enforcer) {
	ctx := context.WithValue(c.Context(), subjectKey, subject)
	ctx = context.WithValue(ctx, enforcerKey, enforcer)
	c.SetContext(ctx)
}

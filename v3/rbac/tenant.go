package rbac

import (
	"context"
	"strings"
	"sync"

	"github.com/gofiber/fiber/v3"
	"github.com/rachmanzz/rbacgo"
)

const tenantKey contextKey = "rbac_tenant"

// TenantRegistry lazily creates and caches one *rbacgo.Enforcer per tenant.
// Concurrent-safe; the factory is invoked at most once per tenant key.
type TenantRegistry struct {
	factory func(tenant string) (*rbacgo.Enforcer, error)
	mu      sync.Mutex
	cache   map[string]*tenantEntry
}

type tenantEntry struct {
	once sync.Once
	enf  *rbacgo.Enforcer
	err  error
}

// NewTenantRegistry instantiates a registry. The factory receives the tenant ID
// and returns the corresponding *rbacgo.Enforcer.
func NewTenantRegistry(factory func(tenant string) (*rbacgo.Enforcer, error)) *TenantRegistry {
	if factory == nil {
		panic(ErrNilTenantRegistry)
	}
	return &TenantRegistry{
		factory: factory,
		cache:   make(map[string]*tenantEntry),
	}
}

// Get retrieves or lazily creates the Enforcer for the given tenant ID.
func (r *TenantRegistry) Get(tenant string) (*rbacgo.Enforcer, error) {
	r.mu.Lock()
	e, ok := r.cache[tenant]
	if !ok {
		e = &tenantEntry{}
		r.cache[tenant] = e
	}
	r.mu.Unlock()

	e.once.Do(func() {
		e.enf, e.err = r.factory(tenant)
	})
	return e.enf, e.err
}

// Clear flushes all cached enforcers from memory.
func (r *TenantRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache = make(map[string]*tenantEntry)
}

// TenantConfig defines configuration options for multi-tenant RBAC.
type TenantConfig struct {
	// Next defines a function to skip this middleware when returning true.
	// Optional. Default: nil
	Next func(c fiber.Ctx) bool

	// Registry manages per-tenant Enforcers.
	// Required.
	Registry *TenantRegistry

	// Tenant extracts the tenant ID from the request.
	// Optional. Default: checks "X-Tenant-ID" header, or c.Locals("tenantId", "tenant_id").
	Tenant func(c fiber.Ctx) (string, error)

	// Subject extracts the authenticated subject ID.
	// Optional. Default: same as ConfigDefault.Subject.
	Subject func(c fiber.Ctx) (string, error)

	// ResourceAction extracts (resource, action) pair.
	// Optional. Default: same as ConfigDefault.ResourceAction.
	ResourceAction func(c fiber.Ctx) (string, string)

	// Unauthorized is executed when tenant or subject extraction fails.
	// Optional. Default: fiber.ErrUnauthorized.
	Unauthorized fiber.Handler

	// Forbidden is executed when enforcement check fails.
	// Optional. Default: fiber.ErrForbidden.
	Forbidden fiber.Handler

	// TenantError handles errors occurring during tenant enforcer resolution.
	// Optional. Default: returns 500 internal server error.
	TenantError func(c fiber.Ctx, err error) error
}

// TenantConfigDefault provides the default configuration for multi-tenant RBAC.
var TenantConfigDefault = TenantConfig{
	Tenant:         defaultTenantExtractor,
	Subject:        defaultSubjectExtractor,
	ResourceAction: defaultResourceAction,
	Unauthorized:   defaultUnauthorizedHandler,
	Forbidden:      defaultForbiddenHandler,
	TenantError:    defaultTenantErrorHandler,
}

func defaultTenantExtractor(c fiber.Ctx) (string, error) {
	if headerVal := strings.TrimSpace(c.Get("X-Tenant-ID")); headerVal != "" {
		return headerVal, nil
	}
	candidates := []string{"tenantId", "tenant_id", "tenant"}
	for _, key := range candidates {
		if val := c.Locals(key); val != nil {
			if strVal, ok := val.(string); ok && strings.TrimSpace(strVal) != "" {
				return strings.TrimSpace(strVal), nil
			}
		}
	}
	return "", ErrTenantNotFound
}

func defaultTenantErrorHandler(c fiber.Ctx, err error) error {
	return fiber.NewError(fiber.StatusInternalServerError, "Failed to resolve tenant RBAC enforcer: "+err.Error())
}

func tenantConfigDefault(cfg ...TenantConfig) TenantConfig {
	if len(cfg) == 0 {
		return TenantConfigDefault
	}
	c := cfg[0]
	if c.Tenant == nil {
		c.Tenant = TenantConfigDefault.Tenant
	}
	if c.Subject == nil {
		c.Subject = TenantConfigDefault.Subject
	}
	if c.ResourceAction == nil {
		c.ResourceAction = TenantConfigDefault.ResourceAction
	}
	if c.Unauthorized == nil {
		c.Unauthorized = TenantConfigDefault.Unauthorized
	}
	if c.Forbidden == nil {
		c.Forbidden = TenantConfigDefault.Forbidden
	}
	if c.TenantError == nil {
		c.TenantError = TenantConfigDefault.TenantError
	}
	return c
}

// NewTenant creates a multi-tenant RBAC middleware handler for Fiber v3.
func NewTenant(config ...TenantConfig) fiber.Handler {
	cfg := tenantConfigDefault(config...)

	if cfg.Registry == nil {
		panic(ErrNilTenantRegistry)
	}

	return func(c fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		tenantID, err := cfg.Tenant(c)
		if err != nil || strings.TrimSpace(tenantID) == "" {
			return cfg.Unauthorized(c)
		}

		enforcer, err := cfg.Registry.Get(tenantID)
		if err != nil {
			return cfg.TenantError(c, err)
		}

		subject, err := cfg.Subject(c)
		if err != nil || strings.TrimSpace(subject) == "" {
			return cfg.Unauthorized(c)
		}

		// Store in locals and context
		c.Locals(string(tenantKey), tenantID)
		c.Locals(string(subjectKey), subject)
		c.Locals(string(enforcerKey), enforcer)

		ctx := context.WithValue(c.Context(), tenantKey, tenantID)
		ctx = context.WithValue(ctx, subjectKey, subject)
		ctx = context.WithValue(ctx, enforcerKey, enforcer)
		c.SetContext(ctx)

		resource, action := cfg.ResourceAction(c)
		if !enforcer.Enforce(c.Context(), subject, resource, action) {
			return cfg.Forbidden(c)
		}

		return c.Next()
	}
}

// RequireTenant creates a route-level multi-tenant guard that enforces a specific (resource, action) permission.
func RequireTenant(resource, action string, config ...TenantConfig) fiber.Handler {
	cfg := tenantConfigDefault(config...)

	if cfg.Registry == nil {
		panic(ErrNilTenantRegistry)
	}

	return func(c fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		tenantID, err := cfg.Tenant(c)
		if err != nil || strings.TrimSpace(tenantID) == "" {
			return cfg.Unauthorized(c)
		}

		enforcer, err := cfg.Registry.Get(tenantID)
		if err != nil {
			return cfg.TenantError(c, err)
		}

		subject, err := cfg.Subject(c)
		if err != nil || strings.TrimSpace(subject) == "" {
			return cfg.Unauthorized(c)
		}

		c.Locals(string(tenantKey), tenantID)
		c.Locals(string(subjectKey), subject)
		c.Locals(string(enforcerKey), enforcer)

		ctx := context.WithValue(c.Context(), tenantKey, tenantID)
		ctx = context.WithValue(ctx, subjectKey, subject)
		ctx = context.WithValue(ctx, enforcerKey, enforcer)
		c.SetContext(ctx)

		if !enforcer.Enforce(c.Context(), subject, resource, action) {
			return cfg.Forbidden(c)
		}

		return c.Next()
	}
}

// TenantFromContext retrieves the tenant ID stored by the tenant RBAC middleware.
func TenantFromContext(c fiber.Ctx) string {
	if val, ok := c.Locals(string(tenantKey)).(string); ok && val != "" {
		return val
	}
	if val, ok := c.Context().Value(tenantKey).(string); ok && val != "" {
		return val
	}
	return ""
}

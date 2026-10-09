package worker

// StarterHookRegistrar defines the hook registration interface used by fiber-starter's cores.AppContracts.
type StarterHookRegistrar interface {
	RegisterBeforeStart(hook func() error)
}

// PostShutdownRegistrar defines the shutdown hook contract matching Fiber v3's app.Hooks().
type PostShutdownRegistrar interface {
	OnPostShutdown(hook func(error) error)
}

// AppHooker defines any application that exposes lifecycle hooks matching Fiber v3.
type AppHooker interface {
	Hooks() PostShutdownRegistrar
}

// StarterHook returns a hook closure suitable for cores.RegisterBeforeStart in fiber-starter.
func StarterHook(cfg ...Config) func() error {
	return func() error {
		InitGlobal(cfg...)
		return nil
	}
}

// ShutdownHook returns a shutdown hook closure conforming to Fiber v3's OnPostShutdown signature.
func ShutdownHook(engine ...*Engine) func(error) error {
	return func(err error) error {
		if len(engine) > 0 && engine[0] != nil {
			return engine[0].Shutdown()
		}
		return Shutdown()
	}
}

// RegisterAppHooks registers graceful shutdown hooks onto any app implementing AppHooker (e.g. *fiber.App)
// without creating a direct dependency on the Fiber package.
func RegisterAppHooks(app AppHooker, engine ...*Engine) {
	if app == nil || app.Hooks() == nil {
		return
	}
	app.Hooks().OnPostShutdown(ShutdownHook(engine...))
}

// RegisterStarterHook registers both PreStart and shutdown hooks conforming to fiber-starter conventions.
// It initializes the global worker engine before server starts, and ensures graceful shutdown when Fiber exits.
func RegisterStarterHook(registrar StarterHookRegistrar, app AppHooker, cfg ...Config) {
	if registrar != nil {
		registrar.RegisterBeforeStart(StarterHook(cfg...))
	}
	if app != nil {
		RegisterAppHooks(app)
	}
}

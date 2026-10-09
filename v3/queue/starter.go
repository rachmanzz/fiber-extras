package queue

// StarterHookRegistrar defines the hook registration contract used by fiber-starter's cores.AppContracts.
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
// It starts the queue consumer for all registered drivers.
func StarterHook(opts ...ConsumerOption) func() error {
	return func() error {
		_, err := StartConsumer(opts...)
		return err
	}
}

// ShutdownHook returns a shutdown hook closure conforming to Fiber v3's OnPostShutdown signature.
func ShutdownHook() func(error) error {
	return func(err error) error {
		StopConsumer()
		return nil
	}
}

// RegisterAppHooks registers graceful shutdown hooks onto any app implementing AppHooker (e.g. *fiber.App)
// without creating a direct dependency on the Fiber package.
func RegisterAppHooks(app AppHooker) {
	if app == nil || app.Hooks() == nil {
		return
	}
	app.Hooks().OnPostShutdown(ShutdownHook())
}

// RegisterStarterHook registers both PreStart and shutdown hooks conforming to fiber-starter conventions.
// It starts the queue consumers before server starts, and ensures graceful shutdown when Fiber exits.
func RegisterStarterHook(registrar StarterHookRegistrar, app AppHooker, opts ...ConsumerOption) {
	if registrar != nil {
		registrar.RegisterBeforeStart(StarterHook(opts...))
	}
	if app != nil {
		RegisterAppHooks(app)
	}
}

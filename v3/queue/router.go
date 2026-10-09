package queue

import (
	"sync"
)

// TopicRoute holds handler and target driver assignment.
type TopicRoute struct {
	Handler    Handler
	DriverName string // Specific driver name (e.g. "postgres", "redis", "nats") or empty for default
}

// Router maps topic strings to their corresponding queue handlers and assigned drivers.
type Router struct {
	mu     sync.RWMutex
	routes map[string]TopicRoute
}

// NewRouter constructs a thread-safe topic router.
func NewRouter() *Router {
	return &Router{
		routes: make(map[string]TopicRoute),
	}
}

// Register binds a topic to a handler with optional configuration.
func (r *Router) Register(topic string, handler Handler, opts ...HandlerOption) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cfg := &handlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	r.routes[topic] = TopicRoute{
		Handler:    handler,
		DriverName: cfg.driverName,
	}
}

// Get retrieves the handler registered for the topic.
func (r *Router) Get(topic string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	route, ok := r.routes[topic]
	return route.Handler, ok
}

// GetRoute retrieves the complete TopicRoute for the topic.
func (r *Router) GetRoute(topic string) (TopicRoute, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	route, ok := r.routes[topic]
	return route, ok
}

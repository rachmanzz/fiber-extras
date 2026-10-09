module github.com/rachmanzz/fiber-extras/v3/queue-redis

go 1.26.0

require (
	github.com/rachmanzz/fiber-extras/v3/queue v0.1.0
	github.com/redis/go-redis/v9 v9.7.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/rachmanzz/fiber-extras/v3/worker v0.1.0 // indirect
)


replace github.com/rachmanzz/fiber-extras/v3/queue => ../queue

replace github.com/rachmanzz/fiber-extras/v3/worker => ../worker

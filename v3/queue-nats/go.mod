module github.com/rachmanzz/fiber-extras/v3/queue-nats

go 1.26.0

require (
	github.com/nats-io/nats.go v1.38.0
	github.com/rachmanzz/fiber-extras/v3/queue v0.1.0
)

require (
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/nats-io/nkeys v0.4.9 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/rachmanzz/fiber-extras/v3/worker v0.1.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
)

replace github.com/rachmanzz/fiber-extras/v3/queue => ../queue

replace github.com/rachmanzz/fiber-extras/v3/worker => ../worker

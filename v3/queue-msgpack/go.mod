module github.com/rachmanzz/fiber-extras/v3/queue-msgpack

go 1.26.0

require (
	github.com/rachmanzz/fiber-extras/v3/queue v0.1.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require (
	github.com/rachmanzz/fiber-extras/v3/worker v0.1.0 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
)

replace github.com/rachmanzz/fiber-extras/v3/queue => ../queue

replace github.com/rachmanzz/fiber-extras/v3/worker => ../worker

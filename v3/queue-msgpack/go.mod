module github.com/rachmanzz/gosuite/v3/queue-msgpack

go 1.26.0

toolchain go1.27.2

require (
	github.com/rachmanzz/gosuite/v3/queue v0.2.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require (
	github.com/rachmanzz/gosuite/v3/worker v0.2.0 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
)

replace github.com/rachmanzz/gosuite/v3/queue => ../queue

replace github.com/rachmanzz/gosuite/v3/worker => ../worker

module github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq

go 1.26.0

require (
	github.com/rabbitmq/amqp091-go v1.15.0
	github.com/rachmanzz/fiber-extras/v3/queue v0.1.0
)

require github.com/rachmanzz/fiber-extras/v3/worker v0.1.0 // indirect

replace github.com/rachmanzz/fiber-extras/v3/queue => ../queue

replace github.com/rachmanzz/fiber-extras/v3/worker => ../worker

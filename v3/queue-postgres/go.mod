module github.com/rachmanzz/fiber-extras/v3/queue-postgres

go 1.26.0

require (
	github.com/jackc/pgx/v5 v5.7.2
	github.com/rachmanzz/fiber-extras/v3/queue v0.1.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/rachmanzz/fiber-extras/v3/worker v0.1.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/text v0.21.0 // indirect
)

replace github.com/rachmanzz/fiber-extras/v3/queue => ../queue

replace github.com/rachmanzz/fiber-extras/v3/worker => ../worker

module github.com/parallelworks/hopper

go 1.27.0

// hopper does no cryptography of its own. Running tests and `go run` in
// fips140=only mode proves that nothing non-approved sneaks in.
godebug fips140=only

require github.com/jackc/pgx/v5 v5.11.0

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

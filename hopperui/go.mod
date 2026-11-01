module github.com/parallelworks/hopper/hopperui

go 1.27.0

toolchain go1.27.3

godebug fips140=only

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/parallelworks/hopper v0.10.3 // x-release-please-version
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)

replace github.com/parallelworks/hopper => ../

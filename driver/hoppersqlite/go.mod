module github.com/parallelworks/hopper/driver/hoppersqlite

go 1.27.0

toolchain go1.27.3

// hopper does no cryptography of its own, and neither does SQLite. Running
// tests in fips140=only mode proves that nothing non-approved sneaks in.
godebug fips140=only

require (
	github.com/parallelworks/hopper v0.7.0 // x-release-please-version
	golang.org/x/sys v0.48.0
	modernc.org/sqlite v1.60.1
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	modernc.org/libc v1.77.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)

replace github.com/parallelworks/hopper => ../../

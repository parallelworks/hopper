// Package hoppermigrate installs and upgrades hopper's database schema.
//
// Migrations are SQL files embedded in this package, versioned and applied in
// order. They run under a cross-process lock taken on a dedicated connection,
// so any number of replicas can call Up at startup. The raw files are also
// available through Migrations for teams that run migrations with their own
// tool; hopper_schema records the applied versions either way.
package hoppermigrate

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"slices"
	"strconv"

	"github.com/parallelworks/hopper/driver"
)

//go:embed migrations/*.sql
var files embed.FS

// Migration is one schema version with its up and down scripts.
type Migration struct {
	Version int
	Name    string
	Up      string
	Down    string
}

var migrations = mustLoad()

// Migrations returns every known migration in ascending version order.
func Migrations() []Migration { return slices.Clone(migrations) }

// Latest returns the newest schema version this package knows.
func Latest() int { return migrations[len(migrations)-1].Version }

// Options tunes a migration run. A nil *Options means the defaults.
type Options struct {
	// Target is the version to stop at. For Up it defaults to the latest
	// version; for Down it defaults to 0, which removes the schema entirely.
	Target int
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Result reports what a migration run did.
type Result struct {
	// Applied lists the versions applied (Up) or reverted (Down), in order.
	Applied []int
	// Version is the schema version afterwards, or 0 if none is installed.
	Version int
}

// Up applies every migration above the current version, up to Target. It is
// safe to call from several processes at once and is a no-op when the schema
// is current.
func Up[TTx any](ctx context.Context, d driver.Driver[TTx], opts *Options) (*Result, error) {
	return run(ctx, d, opts, true)
}

// Down reverts migrations from the current version down to Target. It exists
// for development; production schemas move forward only.
func Down[TTx any](ctx context.Context, d driver.Driver[TTx], opts *Options) (*Result, error) {
	return run(ctx, d, opts, false)
}

// Version returns the installed schema version, or 0 if hopper's schema has
// not been installed.
func Version[TTx any](ctx context.Context, d driver.Driver[TTx]) (int, error) {
	exec, err := d.Migrator().Lock(ctx)
	if err != nil {
		return 0, err
	}
	defer exec.Close(context.WithoutCancel(ctx)) //nolint:errcheck // read-only path
	versions, err := exec.Versions(ctx)
	if err != nil {
		return 0, err
	}
	return current(versions), nil
}

func run[TTx any](ctx context.Context, d driver.Driver[TTx], opts *Options, up bool) (res *Result, err error) {
	if opts == nil {
		opts = &Options{}
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	target := opts.Target
	if up && target == 0 {
		target = Latest()
	}
	if target < 0 || target > Latest() {
		return nil, fmt.Errorf("hoppermigrate: unknown target version %d", target)
	}

	exec, err := d.Migrator().Lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("hoppermigrate: acquire lock: %w", err)
	}
	defer func() {
		// A fresh context so the lock is released even when ctx is cancelled.
		if cerr := exec.Close(context.WithoutCancel(ctx)); cerr != nil && err == nil {
			err = fmt.Errorf("hoppermigrate: release lock: %w", cerr)
		}
	}()

	applied, err := exec.Versions(ctx)
	if err != nil {
		return nil, fmt.Errorf("hoppermigrate: read versions: %w", err)
	}
	res = &Result{Version: current(applied)}

	if up {
		for _, m := range migrations {
			if m.Version > target || slices.Contains(applied, m.Version) {
				continue
			}
			if err := exec.Apply(ctx, m.Version, m.Up, true); err != nil {
				return nil, fmt.Errorf("hoppermigrate: apply %03d_%s: %w", m.Version, m.Name, err)
			}
			logger.InfoContext(ctx, "hopper: applied migration", "version", m.Version, "name", m.Name)
			res.Applied = append(res.Applied, m.Version)
			res.Version = m.Version
		}
		return res, nil
	}

	for _, m := range slices.Backward(migrations) {
		if m.Version <= target || !slices.Contains(applied, m.Version) {
			continue
		}
		if err := exec.Apply(ctx, m.Version, m.Down, false); err != nil {
			return nil, fmt.Errorf("hoppermigrate: revert %03d_%s: %w", m.Version, m.Name, err)
		}
		logger.InfoContext(ctx, "hopper: reverted migration", "version", m.Version, "name", m.Name)
		res.Applied = append(res.Applied, m.Version)
		res.Version = m.Version - 1
	}
	return res, nil
}

func current(applied []int) int {
	if len(applied) == 0 {
		return 0
	}
	return slices.Max(applied)
}

var fileName = regexp.MustCompile(`^(\d+)_([a-z0-9_]+)\.(up|down)\.sql$`)

// mustLoad parses the embedded migration files. It panics on a malformed set,
// which is a build error rather than a runtime condition.
func mustLoad() []Migration {
	byVersion := map[int]*Migration{}
	entries, err := fs.ReadDir(files, "migrations")
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		m := fileName.FindStringSubmatch(e.Name())
		if m == nil {
			panic("hoppermigrate: malformed migration file name " + e.Name())
		}
		version, _ := strconv.Atoi(m[1])
		body, err := fs.ReadFile(files, "migrations/"+e.Name())
		if err != nil {
			panic(err)
		}
		mig := byVersion[version]
		if mig == nil {
			mig = &Migration{Version: version, Name: m[2]}
			byVersion[version] = mig
		}
		if mig.Name != m[2] {
			panic(fmt.Sprintf("hoppermigrate: version %d has two names: %s and %s", version, mig.Name, m[2]))
		}
		if m[3] == "up" {
			mig.Up = string(body)
		} else {
			mig.Down = string(body)
		}
	}
	out := make([]Migration, 0, len(byVersion))
	for _, m := range byVersion {
		if m.Up == "" || m.Down == "" {
			panic(fmt.Sprintf("hoppermigrate: version %d is missing its up or down script", m.Version))
		}
		out = append(out, *m)
	}
	slices.SortFunc(out, func(a, b Migration) int { return a.Version - b.Version })
	for i, m := range out {
		if m.Version != i+1 {
			panic(fmt.Sprintf("hoppermigrate: versions must be contiguous from 1; found %d at position %d", m.Version, i))
		}
	}
	return out
}

package hoppermigrate_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/hoppermigrate"
)

// engineDriver is a driver whose engine has migration scripts of its own,
// which its executor records as they are applied.
type engineDriver struct{ exec *engineExec }

func (engineDriver) Executor() driver.Executor         { return nil }
func (engineDriver) UnwrapTx(struct{}) driver.Executor { return nil }
func (engineDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d engineDriver) Migrator() driver.Migrator       { return d }
func (engineDriver) Listener(context.Context) (driver.Listener, error) {
	return nil, driver.ErrNotSupported
}

func (d engineDriver) Lock(context.Context) (driver.MigrationExecutor, error) { return d.exec, nil }

type engineExec struct {
	versions []int
	applied  []string
	// missing is a version the engine has no script for.
	missing int
}

func (e *engineExec) Versions(context.Context) ([]int, error) { return slices.Clone(e.versions), nil }

func (e *engineExec) Script(version int, up bool) (string, error) {
	if version == e.missing {
		return "", errors.New("no script")
	}
	if up {
		return fmt.Sprintf("engine up %d", version), nil
	}
	return fmt.Sprintf("engine down %d", version), nil
}

func (e *engineExec) Apply(_ context.Context, version int, script string, up bool) error {
	e.applied = append(e.applied, script)
	if up {
		e.versions = append(e.versions, version)
	} else {
		e.versions = slices.DeleteFunc(e.versions, func(v int) bool { return v == version })
	}
	return nil
}

func (*engineExec) Close(context.Context) error { return nil }

// TestEngineScripts migrates an engine that has scripts of its own: Up and
// Down apply its scripts, not the embedded Postgres ones, at hoppermigrate's
// versions, and an engine without a version's script fails the run, naming
// the migration.
func TestEngineScripts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}
	exec := &engineExec{}
	d := engineDriver{exec}
	res, err := hoppermigrate.Up(ctx, d, opts)
	if err != nil || res.Version != hoppermigrate.Latest() {
		t.Fatalf("Up = %+v, %v", res, err)
	}
	for i, script := range exec.applied {
		if want := fmt.Sprintf("engine up %d", i+1); script != want {
			t.Fatalf("script %d = %q, want %q", i, script, want)
		}
	}
	exec.applied = nil
	if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: opts.Logger, Target: hoppermigrate.Latest() - 2}); err != nil {
		t.Fatal(err)
	}
	if want := []string{fmt.Sprintf("engine down %d", hoppermigrate.Latest()), fmt.Sprintf("engine down %d", hoppermigrate.Latest()-1)}; !slices.Equal(exec.applied, want) {
		t.Errorf("Down applied %q, want %q", exec.applied, want)
	}

	exec = &engineExec{missing: 3}
	_, err = hoppermigrate.Up(ctx, engineDriver{exec}, opts)
	if err == nil || !strings.Contains(err.Error(), "003_"+hoppermigrate.Migrations()[2].Name) {
		t.Errorf("Up without a script for version 3 = %v", err)
	}
	if len(exec.applied) != 2 {
		t.Errorf("applied %q before the missing script, want versions 1 and 2", exec.applied)
	}
}

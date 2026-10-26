package hoppersqlite

import (
	"os"
	"path/filepath"
	"testing"
)

// TestResolve checks that resolve follows links as the kernel does: ".."
// after a linked directory leads up from the link's target, not from the
// link, for a file that exists and one that does not yet; and a chain of
// links ends where the last leads.
func TestResolve(t *testing.T) {
	t.Parallel()
	local, remote := t.TempDir(), t.TempDir()
	local, _ = filepath.EvalSymlinks(local)
	remote, _ = filepath.EvalSymlinks(remote)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.Mkdir(filepath.Join(remote, "inner"), 0o755))
	must(os.Symlink(filepath.Join(remote, "inner"), filepath.Join(local, "sub")))
	must(os.WriteFile(filepath.Join(remote, "other.db"), nil, 0o600))
	must(os.Symlink("sub/../other.db", filepath.Join(local, "existing.db")))
	must(os.Symlink("sub/../missing.db", filepath.Join(local, "dangling.db")))
	must(os.Symlink("dangling.db", filepath.Join(local, "chain.db")))
	for link, want := range map[string]string{
		"existing.db":        filepath.Join(remote, "other.db"),
		"dangling.db":        filepath.Join(remote, "missing.db"),
		"chain.db":           filepath.Join(remote, "missing.db"),
		"sub/../new.db":      filepath.Join(remote, "new.db"),
		"sub/./x/../../a.db": "",
		"plain.db":           filepath.Join(local, "plain.db"),
	} {
		got, err := resolve(local + string(filepath.Separator) + link) // not Join, which cleans ".."
		if want == "" {
			if err == nil {
				t.Errorf("resolve(%s) = %s, want an error for a missing directory", link, got)
			}
			continue
		}
		if err != nil || got != want {
			t.Errorf("resolve(%s) = %s, %v; want %s", link, got, err, want)
		}
	}
	must(os.Symlink("loop.db", filepath.Join(local, "loop.db")))
	if got, err := resolve(filepath.Join(local, "loop.db")); err == nil {
		t.Errorf("resolve of a link to itself = %s, want an error", got)
	}
}

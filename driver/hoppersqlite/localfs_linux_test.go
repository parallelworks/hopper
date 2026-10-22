package hoppersqlite

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// setType sets a statfs type, which is an int32, an int64 or a uint32 by
// architecture, to a magic number.
func setType[T int32 | int64 | uint32](t *T, v int64) { *t = T(v) }

// TestNetworkFilesystem checks that a directory on NFS, Lustre, GPFS or
// FUSE is refused, naming the filesystem, and one on a local filesystem is
// not.
func TestNetworkFilesystem(t *testing.T) {
	if err := checkLocal(t.TempDir() + "/hopper.db"); err != nil {
		t.Fatalf("a temporary directory: %v", err)
	}
	defer func(real func(string, *unix.Statfs_t) error) { statfs = real }(statfs)
	for magic, name := range map[int64]string{0x6969: "NFS", 0x0bd00bd0: "Lustre", 0x47504653: "GPFS", 0x65735546: "FUSE", 0xff534d42: "CIFS", 0x7c7c6673: "Parallels", 0xbacbacbc: "VMware"} {
		statfs = func(_ string, st *unix.Statfs_t) error {
			setType(&st.Type, magic)
			return nil
		}
		err := checkLocal("/home/someone/hopper.db")
		if !errors.Is(err, ErrNetworkFilesystem) || !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "/home/someone") {
			t.Errorf("a directory on %s: %v", name, err)
		}
	}
	statfs = func(string, *unix.Statfs_t) error { return unix.ENOENT }
	if err := checkLocal("/nowhere/hopper.db"); err == nil || errors.Is(err, ErrNetworkFilesystem) {
		t.Errorf("a directory that cannot be read: %v", err)
	}
}

// TestNetworkFilesystemThroughLink checks that a database reached through
// a symbolic link is checked, and opened, where the link leads: SQLite
// opens the file a link names and keeps its log and shared memory beside
// it, so a link in a local directory to a database on NFS is refused, as
// one whose directory is a link to NFS is, and a dangling link too, which
// SQLite would follow to create the database.
func TestNetworkFilesystemThroughLink(t *testing.T) {
	local, remote := t.TempDir(), t.TempDir() // remote stands for a directory on NFS
	defer func(real func(string, *unix.Statfs_t) error) { statfs = real }(statfs)
	statfs = func(path string, st *unix.Statfs_t) error {
		if err := unix.Statfs(path, st); err != nil {
			return err
		}
		if path == remote || strings.HasPrefix(path, remote+"/") {
			setType(&st.Type, 0x6969)
		}
		return nil
	}
	refused := func(what, path string) {
		t.Helper()
		d, err := Open(path, nil)
		if err == nil {
			_ = d.Close()
			t.Errorf("%s: opened", what)
			return
		}
		if !errors.Is(err, ErrNetworkFilesystem) || !strings.Contains(err.Error(), remote) {
			t.Errorf("%s: %v", what, err)
		}
	}
	refused("a database on NFS", filepath.Join(remote, "direct.db"))

	link := filepath.Join(local, "dangling.db")
	if err := os.Symlink(filepath.Join(remote, "dangling.db"), link); err != nil {
		t.Fatal(err)
	}
	refused("a dangling link to NFS", link)

	if err := os.WriteFile(filepath.Join(remote, "existing.db"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	link = filepath.Join(local, "existing.db")
	if err := os.Symlink(filepath.Join("..", filepath.Base(remote), "existing.db"), link); err != nil {
		// remote and local share a parent only on some systems
		if err := os.Symlink(filepath.Join(remote, "existing.db"), link); err != nil {
			t.Fatal(err)
		}
	}
	refused("a link to a database on NFS", link)

	dir := filepath.Join(local, "dir")
	if err := os.Symlink(remote, dir); err != nil {
		t.Fatal(err)
	}
	refused("a database in a linked directory on NFS", filepath.Join(dir, "linked.db"))

	if entries, _ := os.ReadDir(remote); len(entries) != 1 {
		t.Errorf("the NFS directory holds %d files, want only the one the test made", len(entries))
	}

	// A link to a local database opens it where the link leads, sharing
	// the write lock of the Driver that opens it by its own path.
	target := filepath.Join(local, "target.db")
	link = filepath.Join(local, "link.db")
	if err := os.Symlink("target.db", link); err != nil {
		t.Fatal(err)
	}
	statfs = unix.Statfs
	d1, err := Open(target, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d1.Close()
	d2, err := Open(link, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if d1.db != d2.db {
		t.Error("a link and its target are two databases to the registry")
	}
	if _, err := os.Stat(target + "-wal"); err != nil {
		t.Errorf("the log is not beside the target: %v", err)
	}
}

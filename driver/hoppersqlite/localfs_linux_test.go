package hoppersqlite

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// TestNetworkFilesystem checks that a directory on NFS, Lustre, GPFS or
// FUSE is refused, naming the filesystem, and one on a local filesystem is
// not.
func TestNetworkFilesystem(t *testing.T) {
	if err := checkLocal(t.TempDir() + "/hopper.db"); err != nil {
		t.Fatalf("a temporary directory: %v", err)
	}
	defer func(real func(string, *unix.Statfs_t) error) { statfs = real }(statfs)
	for magic, name := range map[int64]string{0x6969: "NFS", 0x0bd00bd0: "Lustre", 0x47504653: "GPFS", 0x65735546: "FUSE", 0xff534d42: "CIFS"} {
		statfs = func(_ string, st *unix.Statfs_t) error {
			st.Type = magic
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

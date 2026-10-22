package hoppersqlite

import (
	"errors"
	"fmt"
	"path/filepath"
)

// ErrNetworkFilesystem is returned by Open for a database on a network or
// cluster filesystem (NFS, SMB, Lustre, GPFS and the like, or FUSE, which
// such filesystems often are), where SQLite's file locks and the shared
// memory its write-ahead log needs do not work reliably: two processes
// could both believe they hold the write lock and corrupt the database.
// Home directories on clusters are often on one. Choose a path on a local
// disk instead.
var ErrNetworkFilesystem = errors.New("hoppersqlite: the database is on a network filesystem, where SQLite cannot lock it reliably; choose a path on a local disk")

// checkLocal returns ErrNetworkFilesystem if the directory the database
// file at path is in, where SQLite also keeps its log and shared memory,
// is on a network filesystem.
func checkLocal(path string) error {
	dir := filepath.Dir(path)
	kind, err := networkFilesystem(dir)
	if err != nil {
		return fmt.Errorf("hoppersqlite: tell whether %s is on a local filesystem: %w", dir, err)
	}
	if kind != "" {
		return fmt.Errorf("%w: %s is on %s", ErrNetworkFilesystem, dir, kind)
	}
	return nil
}

package hoppersqlite

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// ErrNetworkFilesystem is returned by Open for a database on a network or
// cluster filesystem (NFS, SMB, Lustre, GPFS and the like, or FUSE, which
// such filesystems often are), where SQLite's file locks and the shared
// memory its write-ahead log needs do not work reliably: two processes
// could both believe they hold the write lock and corrupt the database.
// Home directories on clusters are often on one, and so are the folders a
// virtual machine or container shares with its host, Docker Desktop's bind
// mounts among them. Choose a path on a local disk instead.
var ErrNetworkFilesystem = errors.New("hoppersqlite: the database is on a network filesystem, where SQLite cannot lock it reliably; choose a path on a local disk")

// maxLinks bounds the symbolic links resolve follows, as the kernel's
// ELOOP does.
const maxLinks = 40

// resolve returns the absolute path with every symbolic link in it
// resolved, a last link to a file that does not exist yet included. SQLite
// opens the file a link names and keeps its write-ahead log and shared
// memory beside it, not beside the link, so the path resolved is the one
// to check and to open.
func resolve(path string) (string, error) {
	p, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	for range maxLinks {
		dir, err := filepath.EvalSymlinks(filepath.Dir(p))
		if err != nil {
			return "", err
		}
		p = filepath.Join(dir, filepath.Base(p))
		fi, err := os.Lstat(p)
		if errors.Is(err, fs.ErrNotExist) {
			return p, nil
		}
		if err != nil {
			return "", err
		}
		if fi.Mode()&fs.ModeSymlink == 0 {
			return p, nil
		}
		target, err := os.Readlink(p)
		if err != nil {
			return "", err
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(dir, target)
		}
		p = target
	}
	return "", fmt.Errorf("%s: more than %d symbolic links", path, maxLinks)
}

// checkLocal returns ErrNetworkFilesystem if the database file at path, or
// the directory it is in, where SQLite also keeps its log and shared
// memory, is on a network filesystem. path has its links resolved
// (resolve).
func checkLocal(path string) error {
	dir := filepath.Dir(path)
	check := []string{dir}
	if _, err := os.Stat(path); err == nil {
		check = append(check, path) // a file mounted on its own
	}
	for _, p := range check {
		kind, err := networkFilesystem(p)
		if err != nil {
			return fmt.Errorf("hoppersqlite: tell whether %s is on a local filesystem: %w", p, err)
		}
		if kind != "" {
			return fmt.Errorf("%w: %s is on %s", ErrNetworkFilesystem, p, kind)
		}
	}
	return nil
}

package hoppersqlite

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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
// to check and to open. A path that exists is resolved as the kernel
// resolves it (filepath.EvalSymlinks); one that does not yet, component by
// component, as the kernel would to create it.
func resolve(path string) (string, error) {
	// Not filepath.Abs, which would clean ".." against what precedes it
	// before the links in it are followed.
	p := path
	if !filepath.IsAbs(p) {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		p = wd + string(filepath.Separator) + p
	}
	r, err := filepath.EvalSymlinks(p)
	if err == nil {
		return r, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	return resolveMissing(p)
}

// resolveMissing resolves the links of an absolute path whose last
// component, or the last a link in it leads to, does not exist: each
// component in turn, a link's target in its place, and ".." from the
// directory resolved so far, as the kernel does, not lexically.
func resolveMissing(p string) (string, error) {
	vol := filepath.VolumeName(p)
	cur := vol + string(filepath.Separator)
	rest := split(p[len(vol):])
	links := 0
	for len(rest) > 0 {
		name := rest[0]
		rest = rest[1:]
		switch name {
		case ".":
			continue
		case "..":
			cur = filepath.Dir(cur)
			continue
		}
		next := filepath.Join(cur, name)
		fi, err := os.Lstat(next)
		switch {
		case errors.Is(err, fs.ErrNotExist) && len(rest) == 0:
			return next, nil // the file SQLite will create
		case err != nil:
			return "", err
		case fi.Mode()&fs.ModeSymlink == 0:
			cur = next
			continue
		}
		if links++; links > maxLinks {
			return "", fmt.Errorf("%s: more than %d symbolic links", p, maxLinks)
		}
		target, err := os.Readlink(next)
		if err != nil {
			return "", err
		}
		if filepath.IsAbs(target) {
			vol = filepath.VolumeName(target)
			cur = vol + string(filepath.Separator)
			target = target[len(vol):]
		}
		rest = append(split(target), rest...)
	}
	return cur, nil
}

// split returns the components of a path, without empty ones.
func split(p string) []string {
	return strings.FieldsFunc(p, func(r rune) bool { return r < 0x80 && os.IsPathSeparator(byte(r)) }) //nolint:gosec // r < 0x80
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

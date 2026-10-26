//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !windows

package hoppersqlite

import "errors"

// networkFilesystem cannot tell on this system, and refuses rather than
// risk a database on a filesystem whose locks do not hold.
func networkFilesystem(string) (string, error) {
	return "", errors.New("this system has no way to tell a local filesystem from a network one")
}

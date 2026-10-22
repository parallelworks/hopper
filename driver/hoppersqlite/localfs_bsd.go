//go:build darwin || freebsd || openbsd

package hoppersqlite

import "golang.org/x/sys/unix"

// networkFilesystem reports a filesystem the kernel does not mark local
// (MNT_LOCAL), as NFS, SMB, AFP, WebDAV and FUSE mounts are not.
func networkFilesystem(path string) (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return "", err
	}
	if statfsFlags(&st)&unix.MNT_LOCAL == 0 {
		return "a filesystem not marked local (" + statfsType(&st) + ")", nil
	}
	return "", nil
}

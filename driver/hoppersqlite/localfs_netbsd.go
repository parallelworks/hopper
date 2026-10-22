package hoppersqlite

import "golang.org/x/sys/unix"

// networkFilesystem reports a filesystem the kernel does not mark local
// (MNT_LOCAL), as NFS, SMB and FUSE mounts are not.
func networkFilesystem(path string) (string, error) {
	var st unix.Statvfs_t
	if err := unix.Statvfs(path, &st); err != nil {
		return "", err
	}
	if uint64(st.Flag)&unix.MNT_LOCAL == 0 {
		return "a filesystem not marked local (" + unix.ByteSliceToString(st.Fstypename[:]) + ")", nil
	}
	return "", nil
}

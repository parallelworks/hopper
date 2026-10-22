package hoppersqlite

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// networkMagic names the filesystems, by statfs(2)'s magic number, whose
// locks SQLite cannot rely on: network and cluster filesystems, the shared
// folders of virtual machines, and FUSE, which sshfs, s3fs, GlusterFS and
// many others are.
var networkMagic = map[int64]string{
	0x6969:     "NFS",
	0x517b:     "SMB",
	0xfe534d42: "SMB2",
	0xff534d42: "CIFS",
	0x73757245: "Coda",
	0x5346414f: "AFS",
	0x6b414653: "AFS",
	0x01021997: "9P",
	0x564c:     "NCP",
	0x00c36400: "CephFS",
	0x01161970: "GFS2",
	0x7461636f: "OCFS2",
	0x0bd00bd0: "Lustre",
	0x47504653: "GPFS",
	0x19830326: "BeeGFS",
	0xaad7aaea: "PanFS",
	0x20030528: "OrangeFS",
	0x6a656a63: "virtiofs",
	0x786f4256: "VirtualBox shared folders",
	0x65735546: "FUSE",
}

// statfs is unix.Statfs, which tests replace.
var statfs = unix.Statfs

func networkFilesystem(dir string) (string, error) {
	var st unix.Statfs_t
	if err := statfs(dir, &st); err != nil {
		return "", err
	}
	if name, ok := networkMagic[int64(st.Type)&0xffffffff]; ok { //nolint:unconvert // st.Type is an int32 or a uint32 on some architectures
		return fmt.Sprintf("%s (filesystem type %#x)", name, st.Type), nil
	}
	return "", nil
}

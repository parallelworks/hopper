package hoppersqlite

import "golang.org/x/sys/unix"

func statfsFlags(st *unix.Statfs_t) uint64 { return uint64(st.F_flags) }

func statfsType(st *unix.Statfs_t) string { return unix.ByteSliceToString(st.F_fstypename[:]) }

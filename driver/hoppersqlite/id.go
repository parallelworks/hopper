package hoppersqlite

import (
	"crypto/rand"
	"encoding/binary"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// newID returns a UUIDv7 for a job, batch or message: 48 bits of Unix
// milliseconds at now, 12 bits of the millisecond's fraction, as Postgres
// 18's uuidv7() fills them, so that IDs minted in one process in one
// millisecond mostly sort in order, and 62 random bits. SQLite has no
// generator of its own, so the driver mints IDs where Postgres's does in the
// database. crypto/rand reads the operating system's generator through Go's
// FIPS 140-3 DRBG, as fips140=only requires.
func newID(now time.Time) driver.JobID {
	var id driver.JobID
	ms := uint64(max(now.UnixMilli(), 0))
	fraction := uint64(now.Nanosecond()%int(time.Millisecond)) * 4096 / uint64(time.Millisecond) //nolint:gosec // Nanosecond is in [0, 1e9)
	binary.BigEndian.PutUint64(id[0:8], ms<<16|0x7000|fraction)
	_, _ = rand.Read(id[8:]) // never fails (crypto/rand.Read)
	id[8] = id[8]&0x3f | 0x80
	return id
}

package hoppersqlite

// LockWaiters counts the statements waiting for d's database's write lock,
// in this process or for another process's.
func LockWaiters(d *Driver) int64 { return d.db.waiting.Load() }

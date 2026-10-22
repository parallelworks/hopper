//go:build race

package hoppersqlite_test

// slowdown is how many times longer the race detector makes the driver's
// work, which is mostly SQLite's transpiled C: bounds on how long a
// statement may wait are multiplied by it.
const slowdown = 6

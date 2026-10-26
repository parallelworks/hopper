//go:build !race

package hoppersqlite_test

// slowdown is 1 without the race detector (race_test.go).
const slowdown = 1

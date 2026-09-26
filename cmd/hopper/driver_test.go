package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper/driver/hopperpgx"
)

func newDriver(t *testing.T, url string) *hopperpgx.Driver {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return hopperpgx.New(pool)
}

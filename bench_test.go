package main

import "testing"

func BenchmarkSqlite(b *testing.B) {
	BenchmarkDrivers(b)
}

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	_ "modernc.org/sqlite"
)

type BenchmarkResult struct {
	Driver    string
	Operation string
	DataSize  int
	Duration  time.Duration
}

// func main() {
// 	dataSizes := []int{64, 256, 1024, 4096, 1024 * 1024} // in bytes
//
// 	results := []BenchmarkResult{}
//
// 	drivers := map[string]string{
// 		"modernc": "sqlite",
// 		"mattn":   "sqlite3",
// 	}
//
// 	for driverName, driverImport := range drivers {
// 		for _, dataSize := range dataSizes {
// 			writeDuration := benchmarkWrite(driverImport, dataSize)
// 			readDuration := benchmarkRead(driverImport, dataSize)
//
// 			results = append(results, BenchmarkResult{Driver: driverName, Operation: "write", DataSize: dataSize, Duration: writeDuration})
// 			results = append(results, BenchmarkResult{Driver: driverName, Operation: "read", DataSize: dataSize, Duration: readDuration})
//
// 			log.Printf("Driver: %s, Operation: write, DataSize: %d bytes, Duration: %v\n", driverName, dataSize, writeDuration)
// 			log.Printf("Driver: %s, Operation: read, DataSize: %d bytes, Duration: %v\n", driverName, dataSize, readDuration)
// 		}
// 	}
//
// 	saveResultsToCSV(results)
// }

func benchmarkWrite(driver string, dataSize int) time.Duration {
	db, err := sql.Open(driver, "file::memory:?cache=shared")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE test (data BLOB)")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	data := make([]byte, dataSize)

	start := time.Now()
	for i := 0; i < 100; i++ { // Number of insert operations
		_, err := db.ExecContext(ctx, "INSERT INTO test (data) VALUES (?)", data)
		if err != nil {
			log.Fatalf("Failed to insert data: %v", err)
		}
	}
	duration := time.Since(start)

	return duration
}

func benchmarkRead(driver string, dataSize int) time.Duration {
	db, err := sql.Open(driver, "file::memory:?cache=shared")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE test (data BLOB)")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	data := make([]byte, dataSize)
	for i := 0; i < 100; i++ { // Insert data for reading
		_, err := db.ExecContext(ctx, "INSERT INTO test (data) VALUES (?)", data)
		if err != nil {
			log.Fatalf("Failed to insert data: %v", err)
		}
	}

	start := time.Now()
	for i := 0; i < 100; i++ { // Number of read operations
		rows, err := db.QueryContext(ctx, "SELECT data FROM test LIMIT 1")
		if err != nil {
			log.Fatalf("Failed to query data: %v", err)
		}
		rows.Close()
	}
	duration := time.Since(start)

	return duration
}

func saveResultsToCSV(results []BenchmarkResult) {
	file, err := os.Create("benchmark_results.csv")
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer file.Close()

	fmt.Fprintln(file, "Driver,Operation,DataSize,Duration")

	for _, result := range results {
		fmt.Fprintf(file, "%s,%s,%d,%v\n", result.Driver, result.Operation, result.DataSize, result.Duration)
	}
}

// Benchmark functions
func BenchmarkWrite(b *testing.B, driver string, dataSize int) {
	b.Helper()

	db, err := sql.Open(driver, "file::memory:?cache=shared")
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE test (data BLOB)")
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	data := make([]byte, dataSize)

	for i := 0; i < b.N; i++ {
		_, err := db.ExecContext(ctx, "INSERT INTO test (data) VALUES (?)", data)
		if err != nil {
			b.Fatalf("Failed to insert data: %v", err)
		}
	}
}

func BenchmarkRead(b *testing.B, driver string, dataSize int) {
	b.Helper()

	db, err := sql.Open(driver, "file::memory:?cache=shared")
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE test (data BLOB)")
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	data := make([]byte, dataSize)
	for i := 0; i < 100; i++ { // Insert data for reading
		_, err := db.ExecContext(ctx, "INSERT INTO test (data) VALUES (?)", data)
		if err != nil {
			b.Fatalf("Failed to insert data: %v", err)
		}
	}

	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, "SELECT data FROM test LIMIT 1")
		if err != nil {
			b.Fatalf("Failed to query data: %v", err)
		}
		rows.Close()
	}
}

func BenchmarkDrivers(b *testing.B) {
	dataSizes := []int{64, 256, 1024, 4096, 1024 * 1024}

	drivers := map[string]string{
		"modernc": "sqlite",
		"mattn":   "sqlite3",
	}

	for driverName, driverImport := range drivers {
		for _, dataSize := range dataSizes {
			b.Run(fmt.Sprintf("%s_Write_%dBytes", driverName, dataSize), func(b *testing.B) {
				BenchmarkWrite(b, driverImport, dataSize)
			})
			b.Run(fmt.Sprintf("%s_Read_%dBytes", driverName, dataSize), func(b *testing.B) {
				BenchmarkRead(b, driverImport, dataSize)
			})
		}
	}
}

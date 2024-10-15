// This is custom goose binary with sqlite3 support only.

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/nameindexer/pkg/clickhouse/migrations"
)

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: migrations <dbstring> <command> [args]")
		return
	}

	dbstring := args[1]
	fmt.Println(dbstring)
	dbOptions, err := clickhouse.ParseDSN(dbstring)
	if err != nil {
		log.Fatalf("goose: failed to parse DB string: %v\n", err)
	}
	db := clickhouse.OpenDB(dbOptions)

	err = migrations.RunGoose(context.Background(), args[2:], db)
	if err != nil {
		_ = db.Close()
		log.Fatalf("goose: failed to run goose: %v\n", err)
	}
	if err := db.Close(); err != nil {
		log.Fatalf("goose: failed to close DB: %v\n", err)
	}
}

// Code generated by "clickhouse-infra" DO NOT EDIT.
package migrations

import (
	"context"
	"database/sql"

	"github.com/DIMO-Network/clickhouse-infra/pkg/migrate"
)

// registerFuncs is a list of functions that register migrations.
// Each migration file should have an init function that appends their register function to this list.
// This is different from the goose registration which is public for all packages.
var registerFuncs = []func(){}

// RegisterFuncs returns the list of functions for registering goose migrations.
func RegisterFuncs() []func() {
	return registerFuncs
}

// RunGoose runs the goose command with the provided arguments.
// args should be the command and the arguments to pass to goose.
// eg RunGoose(ctx, []string{"up", "-v"}, db).
func RunGoose(ctx context.Context, gooseArgs []string, db *sql.DB) error {
	return migrate.RunGoose(ctx, gooseArgs, RegisterFuncs(), db)
}

package migrations

import (
	"context"
	"database/sql"
	"runtime"

	"github.com/pressly/goose/v3"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	registerFunc := func() { goose.AddNamedMigrationContext(filename, upAddOptional, downAddOptional) }
	registerFuncs = append(registerFuncs, registerFunc)
}

func upAddOptional(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	upStatements := []string{
		// add col called optional
		`ALTER TABLE name_index ADD COLUMN IF NOT EXISTS optional String COMMENT 'Optional metadata'`,
	}
	for _, upStatement := range upStatements {
		_, err := tx.ExecContext(ctx, upStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

func downAddOptional(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	downStatements := []string{
		`ALTER TABLE name_index DROP COLUMN IF EXISTS optional`,
	}
	for _, downStatement := range downStatements {
		_, err := tx.ExecContext(ctx, downStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

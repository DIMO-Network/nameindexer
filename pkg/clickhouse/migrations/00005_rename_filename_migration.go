package migrations

import (
	"context"
	"database/sql"
	"runtime"

	"github.com/pressly/goose/v3"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	registerFunc := func() { goose.AddNamedMigrationContext(filename, upRenameFilename, downRenameFilename) }
	registerFuncs = append(registerFuncs, registerFunc)
}

func upRenameFilename(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	upStatements := []string{
		"ALTER TABLE name_index RENAME COLUMN file_name TO index_key;",
		"ALTER TABLE name_index MODIFY COLUMN index_key String AFTER optional;",
	}
	for _, upStatement := range upStatements {
		_, err := tx.ExecContext(ctx, upStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

func downRenameFilename(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	downStatements := []string{
		"ALTER TABLE name_index RENAME COLUMN index_key TO file_name;",
		"MODIFY COLUMN name_index file_name String BEFORE optional;",
	}
	for _, downStatement := range downStatements {
		_, err := tx.ExecContext(ctx, downStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

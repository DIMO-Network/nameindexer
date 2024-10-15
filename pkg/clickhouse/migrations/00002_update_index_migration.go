package migrations

import (
	"context"
	"database/sql"
	"runtime"

	"github.com/pressly/goose/v3"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	registerFunc := func() { goose.AddNamedMigrationContext(filename, upUpdateIndex, downUpdateIndex) }
	registerFuncs = append(registerFuncs, registerFunc)
	registerFunc()
}

func upUpdateIndex(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	upStatements := []string{
		`CREATE TABLE IF NOT EXISTS name_index_tmp AS name_index ENGINE = MergeTree()
			ORDER BY (timestamp, primary_filler, data_type, subject, secondary_filler);
		`,
		`INSERT INTO name_index_tmp SELECT * FROM name_index;`,
		`DROP TABLE IF EXISTS name_index;`,
		`RENAME TABLE name_index_tmp TO name_index;`,
	}
	for _, upStatement := range upStatements {
		_, err := tx.ExecContext(ctx, upStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

func downUpdateIndex(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	downStatements := []string{
		`CREATE TABLE IF NOT EXISTS name_index_tmp AS name_index ENGINE = MergeTree()
			ORDER BY file_name
		`,
		`INSERT INTO name_index_tmp SELECT * FROM name_index`,
		`DROP TABLE IF EXISTS name_index`,
		`RENAME TABLE name_index_tmp TO name_index`,
	}
	for _, downStatement := range downStatements {
		_, err := tx.ExecContext(ctx, downStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

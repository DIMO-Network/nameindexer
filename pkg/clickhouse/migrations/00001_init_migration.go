package migrations

import (
	"context"
	"database/sql"
	"runtime"

	"github.com/pressly/goose/v3"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	registerFunc := func() { goose.AddNamedMigrationContext(filename, upInit, downInit) }
	registerFuncs = append(registerFuncs, registerFunc)
	registerFunc()
}

func upInit(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	upStatements := []string{
		createNameIndexStmt,
	}
	for _, upStatement := range upStatements {
		_, err := tx.ExecContext(ctx, upStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

func downInit(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	downStatements := []string{
		`DROP TABLE IF EXISTS name_index;`,
	}
	for _, downStatement := range downStatements {
		_, err := tx.ExecContext(ctx, downStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

const createNameIndexStmt = `
CREATE TABLE IF NOT EXISTS name_index
(
    timestamp DateTime('UTC') COMMENT 'Combined date and time in UTC with millisecond precision.',
    primary_filler FixedString(2) COMMENT 'Primary filler, a constant string of length 2.',
    data_type FixedString(10) COMMENT 'Data type left-padded with zeros or truncated to 10 characters.',
    subject FixedString(40) COMMENT 'Hexadecimal representation of the device\'s address or tokenId 40 characters.',
    secondary_filler FixedString(2) COMMENT 'Secondary filler, a constant string of length 2.',
	file_name String COMMENT 'Name of the file that the data was collected from.',
)
ENGINE = MergeTree()
ORDER BY (toDate(timestamp), primary_filler, data_type, subject, secondary_filler, timestamp);
`

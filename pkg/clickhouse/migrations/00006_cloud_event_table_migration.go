package migrations

import (
	"context"
	"database/sql"
	"runtime"

	"github.com/pressly/goose/v3"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	registerFunc := func() { goose.AddNamedMigrationContext(filename, upCloud_event_table, downCloud_event_table) }
	registerFuncs = append(registerFuncs, registerFunc)
}

func upCloud_event_table(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	upStatements := []string{`
CREATE TABLE IF NOT EXISTS cloud_event (
    subject String COMMENT 'identifying the subject of the event within the context of the event producer',
    event_time DateTime64(3, 'UTC') COMMENT 'Time at which the event occurred.',
    event_type String COMMENT 'event type for this object',
    id String COMMENT 'Identifier for the event.',
    source String COMMENT 'Entity that is responsible for providing this cloud event',
    producer String COMMENT 'specific instance, process or device that creates the data structure describing the cloud event.',
    data_content_type String COMMENT 'Type of data of this object.',
    data_version String COMMENT 'Version of the data stored for this cloud event.',
	extras String COMMENT 'Extra metadata for the cloud event',
    index_key String COMMENT 'Key of the backing object for this cloud event'
) ENGINE = MergeTree()
ORDER BY
    (subject, event_time, event_type) SETTINGS index_granularity = 8192;`,
	}
	for _, upStatement := range upStatements {
		_, err := tx.ExecContext(ctx, upStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

func downCloud_event_table(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	downStatements := []string{
		"DROP TABLE IF EXISTS cloud_event;",
	}
	for _, downStatement := range downStatements {
		_, err := tx.ExecContext(ctx, downStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

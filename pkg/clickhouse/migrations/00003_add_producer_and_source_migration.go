package migrations

import (
	"context"
	"database/sql"
	"runtime"

	"github.com/pressly/goose/v3"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	registerFunc := func() { goose.AddNamedMigrationContext(filename, upAddProducerAndSource, downAddProducerAndSource) }
	registerFuncs = append(registerFuncs, registerFunc)
	registerFunc()
}

func upAddProducerAndSource(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	upStatements := []string{
		addProducerAndSourceCreateStmt,
		`
INSERT INTO name_index_tmp (subject, timestamp, primary_filler, source, data_type, secondary_filler, producer, file_name)
SELECT 
    subject, 
    timestamp,
    primary_filler,
    '' AS source, 
    data_type,
    secondary_filler,
    '' AS producer,
    file_name
FROM name_index;
		`,
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

func downAddProducerAndSource(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	downStatements := []string{
		`
CREATE TABLE IF NOT EXISTS name_index_tmp
(
    timestamp DateTime('UTC') COMMENT 'Combined date and time in UTC with millisecond precision.',
    primary_filler FixedString(2) COMMENT 'Primary filler, a constant string of length 2.',
    data_type FixedString(10) COMMENT 'Data type left-padded with zeros or truncated to 10 characters.',
    subject FixedString(40) COMMENT 'Hexadecimal representation of the device\'s address or tokenId 40 characters.',
    secondary_filler FixedString(2) COMMENT 'Secondary filler, a constant string of length 2.',
	file_name String COMMENT 'Name of the file that the data was collected from.',
)
ENGINE = MergeTree()
ORDER BY (timestamp, primary_filler, data_type, subject, secondary_filler);
`,
		` 
INSERT INTO name_index_tmp (timestamp, primary_filler, data_type, subject, secondary_filler, file_name)
SELECT 
    timestamp,
    left(primary_filler, 2),
    left(data_type, 10),
    left(subject, 40),
    left(secondary_filler, 2),
    file_name
FROM name_index;
`,
		`DROP TABLE IF EXISTS name_index;`,
		`RENAME TABLE name_index_tmp TO name_index;`,
	}
	for _, downStatement := range downStatements {
		_, err := tx.ExecContext(ctx, downStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

var addProducerAndSourceCreateStmt = `
CREATE TABLE IF NOT EXISTS name_index_tmp
(
    subject FixedString(64) COMMENT 'represents the NFT DID of the subject of the event ChainId+ContractAddress+TokenId.',
    timestamp DateTime('UTC') COMMENT 'Combined date and time in UTC with millisecond precision.',
    primary_filler FixedString(2) COMMENT 'Primary filler, a constant string of length 2.',
	source FixedString(40) COMMENT 'represents the address of the source of the event.',
    data_type FixedString(20) COMMENT 'Data type left-padded with zeros or truncated to 20 characters.',
    secondary_filler FixedString(2) COMMENT 'Secondary filler, a constant string of length 2.',
	producer FixedString(64) COMMENT 'represents the NFT DID of the producer of the event ChainId+ContractAddress+TokenId.',
	file_name String COMMENT 'Name of the file that the data was collected from.',
)
ENGINE = MergeTree()
ORDER BY (subject, timestamp, primary_filler, source, data_type, secondary_filler, producer);
`

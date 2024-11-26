package clickhouse

import (
	"fmt"

	"github.com/DIMO-Network/nameindexer"
)

const (
	// TableName is the name of the table in Clickhouse.
	TableName = "name_index"
	// TimestampColumn is the name of the timestamp column in Clickhouse.
	TimestampColumn = "timestamp"
	// PrimaryFillerColumn is the name of the primary filler column in Clickhouse.
	PrimaryFillerColumn = "primary_filler"
	// DataTypeColumn is the name of the data type column in Clickhouse.
	DataTypeColumn = "data_type"
	// SubjectColumn is the name of the subject column in Clickhouse.
	SubjectColumn = "subject"
	// SecondaryFillerColumn is the name of the secondary filler column in Clickhouse.
	SecondaryFillerColumn = "secondary_filler"
	// IndexKeyColumn is the name of the index name column in Clickhouse.
	IndexKeyColumn = "index_key"
	// SourceColumn is the name of the source column in Clickhouse.
	SourceColumn = "source"
	// ProducerColumn is the name of the producer column in Clickhouse.
	ProducerColumn = "producer"
	// OptionalColumn is the name of the optional column in Clickhouse.
	OptionalColumn = "optional"

	// InsertStmt is the SQL statement for inserting a row into Clickhouse.
	InsertStmt = "INSERT INTO " + TableName + " (" +
		SubjectColumn + ", " +
		TimestampColumn + ", " +
		PrimaryFillerColumn + ", " +
		SourceColumn + ", " +
		DataTypeColumn + ", " +
		SecondaryFillerColumn + ", " +
		ProducerColumn + ", " +
		OptionalColumn + ", " +
		IndexKeyColumn +
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

// IndexToSlice converts a Inedx to an array of any for Clickhouse insertion.
// The order of the elements in the array match the order of the columns in the table.
func IndexToSlice(origIndex *nameindexer.Index) ([]any, error) {
	index := origIndex.WithEncodedParts()
	indexKey, err := nameindexer.EncodeIndex(origIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to encode index: %w", err)
	}
	return []any{
		index.Subject,         // Vehicle or Device DID
		index.Timestamp,       // Timestamp
		index.PrimaryFiller,   // DIMO event type (status, fingerprint, connectivity)
		index.Source,          // Source Ethereum address
		index.DataType,        // DataVersion
		index.SecondaryFiller, // Secondary filler
		index.Producer,        // Producer DID
		index.Optional,        // Optional
		indexKey,
	}, nil
}

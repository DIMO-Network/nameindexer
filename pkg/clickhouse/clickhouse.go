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
	// FileNameColumn is the name of the file name column in Clickhouse.
	FileNameColumn = "file_name"
	// SourceColumn is the name of the source column in Clickhouse.
	SourceColumn = "source"
	// ProducerColumn is the name of the producer column in Clickhouse.
	ProducerColumn = "producer"

	// InsertStmt is the SQL statement for inserting a row into Clickhouse.
	InsertStmt = "INSERT INTO " + TableName + " (" +
		SubjectColumn + ", " +
		TimestampColumn + ", " +
		PrimaryFillerColumn + ", " +
		SourceColumn + ", " +
		DataTypeColumn + ", " +
		SecondaryFillerColumn + ", " +
		ProducerColumn + ", " +
		FileNameColumn +
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	// InsertStm = fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", TableName, TimestampColumn, PrimaryFillerColumn, DataTypeColumn, SubjectColumn, SecondaryFillerColumn, SourceColumn, FileNameColumn, ProducerColumn)
)

// IndexToSlice converts a Inedx to an array of any for Clickhouse insertion.
// This function will modify the index to have correctly padded values.
// The order of the elements in the array match the order of the columns in the table.
func IndexToSlice(origIndex *nameindexer.Index) ([]any, error) {
	index, err := nameindexer.SetDefaultsAndValidateIndex(origIndex)
	if err != nil {
		return nil, fmt.Errorf("set defaults and validate index: %w", err)
	}
	fileName, err := nameindexer.EncodeIndex(origIndex)
	if err != nil {
		return nil, fmt.Errorf("encode index: %w", err)
	}
	source := nameindexer.EncodeAddress(index.Source)
	subject, err := nameindexer.EncodeNFTDID(index.Subject)
	if err != nil {
		return nil, fmt.Errorf("encode subject: %w", err)
	}
	producer, err := nameindexer.EncodeNFTDID(index.Producer)
	if err != nil {
		return nil, fmt.Errorf("encode producer: %w", err)
	}
	return []any{
		subject,               // Vehicle or Device DID
		index.Timestamp,       // Timestamp
		index.PrimaryFiller,   // DIMO event type (status, fingerprint, connectivity)
		source,                // Source Ethereum address
		index.DataType,        // DataVersion
		index.SecondaryFiller, // Secondary filler
		producer,              // Producer DID
		fileName,
	}, nil
}

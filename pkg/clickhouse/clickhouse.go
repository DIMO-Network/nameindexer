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
)

// IndexToSlice converts a Inedx to an array of any for Clickhouse insertion.
// This function will modify the index to have correctly padded values.
// The order of the elements in the array match the order of the columns in the table.
func IndexToSlice(index *nameindexer.Index) ([]any, error) {
	fileName, err := nameindexer.EncodeIndex(index)
	if err != nil {
		return nil, fmt.Errorf("encode index: %w", err)
	}

	return []any{
		index.Timestamp,
		index.PrimaryFiller,
		index.DataType,
		index.Subject,
		index.SecondaryFiller,
		fileName,
	}, nil
}

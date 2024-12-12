package clickhouse

import (
	"encoding/json"
	"fmt"
	"time"

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
	key, err := nameindexer.EncodeIndex(origIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to encode index: %w", err)
	}
	return IndexToSliceWithKey(origIndex, key), nil
}

// IndexToSliceWithKey converts a Inedx to an array of any for Clickhouse insertion.
// This function allows to pass the key as a parameter instead of encoding it from the index.
func IndexToSliceWithKey(origIndex *nameindexer.Index, key string) []any {
	index := origIndex.WithEncodedParts()
	return []any{
		index.Subject,         // Vehicle or Device DID
		index.Timestamp,       // Timestamp
		index.PrimaryFiller,   // DIMO event type (status, fingerprint, connectivity)
		index.Source,          // Source Ethereum address
		index.DataType,        // DataVersion
		index.SecondaryFiller, // Secondary filler
		index.Producer,        // Producer DID
		index.Optional,        // Optional
		key,                   // Index key
	}
}

// UnmarshalIndexSlice unmarshals a byte slice into an array of any for Clickhouse insertion.
func UnmarshalIndexSlice(jsonArray []byte) ([]any, error) {
	rawSlice := []json.RawMessage{}
	err := json.Unmarshal(jsonArray, &rawSlice)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal index slice: %w", err)
	}
	if len(rawSlice) != 9 {
		return nil, fmt.Errorf("invalid index slice length: %d", len(rawSlice))
	}
	var subject string
	var timestamp time.Time
	var primaryFiller string
	var source string
	var dataType string
	var secondaryFiller string
	var producer string
	var optional string
	var indexKey string
	err = json.Unmarshal(rawSlice[0], &subject)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal subject: %w", err)
	}
	err = json.Unmarshal(rawSlice[1], &timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal timestamp: %w", err)
	}
	err = json.Unmarshal(rawSlice[2], &primaryFiller)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal primary filler: %w", err)
	}
	err = json.Unmarshal(rawSlice[3], &source)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal source: %w", err)
	}
	err = json.Unmarshal(rawSlice[4], &dataType)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data type: %w", err)
	}
	err = json.Unmarshal(rawSlice[5], &secondaryFiller)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal secondary filler: %w", err)
	}
	err = json.Unmarshal(rawSlice[6], &producer)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal producer: %w", err)
	}
	err = json.Unmarshal(rawSlice[7], &optional)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal optional: %w", err)
	}
	err = json.Unmarshal(rawSlice[8], &indexKey)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal index key: %w", err)
	}
	return []any{subject, timestamp, primaryFiller, source, dataType, secondaryFiller, producer, optional, indexKey}, nil
}

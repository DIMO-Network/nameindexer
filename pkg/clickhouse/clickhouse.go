package clickhouse

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/nameindexer"
)

const (
	// TableName is the name of the table in Clickhouse.
	TableName = "cloud_event"
	// SubjectColumn is the name of the subject column in Clickhouse.
	SubjectColumn = "subject"
	// TimestampColumn is the name of the timestamp column in Clickhouse.
	TimestampColumn = "event_time"
	// TypeColumn is the name of the cloud event type column in Clickhouse.
	TypeColumn = "event_type"
	// IDColumn is the name of the ID column in Clickhouse.
	IDColumn = "id"
	// SourceColumn is the name of the source column in Clickhouse.
	SourceColumn = "source"
	// ProducerColumn is the name of the producer column in Clickhouse.
	ProducerColumn = "producer"
	// DataContentTypeColumn is the name of the data content type column in Clickhouse.
	DataContentTypeColumn = "data_content_type"
	// DataVersionColumn is the name of the data version column in Clickhouse.
	DataVersionColumn = "data_version"
	// ExtraColumn is the name of the extra column in Clickhouse.
	ExtrasColumn = "extras"
	// IndexKeyColumn is the name of the index name column in Clickhouse.
	IndexKeyColumn = "index_key"

	// InsertStmt is the SQL statement for inserting a row into Clickhouse.
	InsertStmt = "INSERT INTO " + TableName + " (" +
		SubjectColumn + ", " +
		TimestampColumn + ", " +
		TypeColumn + ", " +
		IDColumn + ", " +
		SourceColumn + ", " +
		ProducerColumn + ", " +
		DataContentTypeColumn + ", " +
		DataVersionColumn + ", " +
		ExtrasColumn + ", " +
		IndexKeyColumn +
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

// CloudEventToSlice converts a CloudEvent to an array of any for Clickhouse insertion.
// The order of the elements in the array match the order of the columns in the table.
func CloudEventToSlice(event *cloudevent.CloudEventHeader) []any {
	idx := nameindexer.CloudEventToPartialIndex(event)
	key, err := nameindexer.EncodeIndex(&idx)
	if err != nil {
		// hash header if index key is too long\
		key = fmt.Sprintf("%s_%s_%s_%s", event.ID, event.Source, event.Time.Format(time.RFC3339), event.Subject)
	}
	jsonExtra, _ := json.Marshal(event.Extras)
	return []any{
		event.Subject,
		event.Time,
		event.Type,
		event.ID,
		event.Source,
		event.Producer,
		event.DataContentType,
		event.DataVersion,
		string(jsonExtra),
		key,
	}
}

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
func IndexToSliceWithKey(index *nameindexer.Index, key string) []any {
	return []any{
		index.Subject,   // Vehicle or Device DID
		index.Timestamp, // Timestamp
		nameindexer.FillerToCloudType(index.PrimaryFiller), // DIMO event type (status, fingerprint, connectivity)
		"",                 // Event ID
		index.Source,       // Source Ethereum address
		index.Producer,     // Producer DID
		"application/json", // DataContentType
		index.DataType,     // DataVersion
		index.Optional,     // Extra metadata
		key,                // Index key
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

// UnmarshalCloudEventSlice unmarshals a byte slice into an array of any for Clickhouse insertion.
func UnmarshalCloudEventSlice(jsonArray []byte) ([]any, error) {
	rawSlice := []json.RawMessage{}
	err := json.Unmarshal(jsonArray, &rawSlice)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal cloud event slice: %w", err)
	}
	if len(rawSlice) != 10 {
		return nil, fmt.Errorf("invalid cloud event slice length: %d", len(rawSlice))
	}
	var subject string
	var timestamp time.Time
	var eventType string
	var id string
	var source string
	var producer string
	var dataContentType string
	var dataVersion string
	var extras string
	var indexKey string
	err = json.Unmarshal(rawSlice[0], &subject)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal subject: %w", err)
	}
	err = json.Unmarshal(rawSlice[1], &timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal timestamp: %w", err)
	}
	err = json.Unmarshal(rawSlice[2], &eventType)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal event type: %w", err)
	}
	err = json.Unmarshal(rawSlice[3], &id)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal id: %w", err)
	}
	err = json.Unmarshal(rawSlice[4], &source)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal source: %w", err)
	}
	err = json.Unmarshal(rawSlice[5], &producer)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal producer: %w", err)
	}
	err = json.Unmarshal(rawSlice[6], &dataContentType)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data content type: %w", err)
	}
	err = json.Unmarshal(rawSlice[7], &dataVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data version: %w", err)
	}
	err = json.Unmarshal(rawSlice[8], &extras)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal extras: %w", err)
	}
	err = json.Unmarshal(rawSlice[9], &indexKey)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal index key: %w", err)
	}
	return []any{subject, timestamp, eventType, id, source, producer, dataContentType, dataVersion, extras, indexKey}, nil
}

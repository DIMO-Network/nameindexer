// Package indexrepo contains service code for gettting and managing indexed objects.
package indexrepo

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/nameindexer"
	chindexer "github.com/DIMO-Network/nameindexer/pkg/clickhouse"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/volatiletech/sqlboiler/v4/drivers"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
)

// Service manages and retrieves data messages from indexed objects in S3.
type Service struct {
	objGetter ObjectGetter
	chConn    clickhouse.Conn
}

type CloudEventMetadata struct {
	cloudevent.CloudEventHeader
	Key string
}

// ObjectGetter is an interface for getting an object from S3.
type ObjectGetter interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// New creates a new instance of serviceService.
func New(chConn clickhouse.Conn, objGetter ObjectGetter) *Service {
	return &Service{
		objGetter: objGetter,
		chConn:    chConn,
	}
}

// getLatestKeyFromRaw returns the latest key for the given subject and data type.
func (s *Service) getLatestKeyFromRaw(ctx context.Context, opts RawSearchOptions) (string, error) {
	mods := []qm.QueryMod{
		qm.Select("argMax(" + chindexer.IndexKeyColumn + ", " + chindexer.TimestampColumn + ") AS index_key"),
		qm.From(chindexer.TableName),
	}
	optsMods, err := opts.QueryMods()
	if err != nil {
		return "", err
	}
	mods = append(mods, optsMods...)
	query, args := newQuery(mods...)
	var key string
	err = s.chConn.QueryRow(ctx, query, args...).Scan(&key)
	if err != nil {
		return "", fmt.Errorf("failed to get latest key: %w", err)
	}
	if key == "" {
		return "", fmt.Errorf("no keys found for subject %w", sql.ErrNoRows)
	}
	return key, nil
}

// GetLatestMetadata returns the cloud event metadata for the latest event that matches the given options.
func (s *Service) GetLatestMetadata(ctx context.Context, opts SearchOptions) (CloudEventMetadata, error) {
	return s.GetLatestMetadataFromRaw(ctx, opts.ToRawSearchOptions())
}

// GetLatestMetadataFromRaw returns the latest cloud event metadata that matches the given options.
func (s *Service) GetLatestMetadataFromRaw(ctx context.Context, opts RawSearchOptions) (CloudEventMetadata, error) {
	opts.TimestampAsc = false
	events, err := s.ListMetadataFromRaw(ctx, 1, opts)
	if err != nil {
		return CloudEventMetadata{}, err
	}
	return events[0], nil
}

// ListMetadata fetches and returns a list of metadata for cloud events that match the given options.
func (s *Service) ListMetadata(ctx context.Context, limit int, opts SearchOptions) ([]CloudEventMetadata, error) {
	return s.ListMetadataFromRaw(ctx, limit, opts.ToRawSearchOptions())
}

// ListMetadataFromRaw fetches and returns a list of metadata for cloud events that match the given options.
func (s *Service) ListMetadataFromRaw(ctx context.Context, limit int, opts RawSearchOptions) ([]CloudEventMetadata, error) {
	order := " DESC"
	if opts.TimestampAsc {
		order = " ASC"
	}
	mods := []qm.QueryMod{
		qm.Select(chindexer.SubjectColumn),
		qm.Select(chindexer.TimestampColumn),
		qm.Select(chindexer.PrimaryFillerColumn),
		qm.Select(chindexer.SourceColumn),
		qm.Select(chindexer.DataTypeColumn),
		qm.Select(chindexer.ProducerColumn),
		qm.Select(chindexer.IndexKeyColumn),
		qm.From(chindexer.TableName),
		qm.OrderBy(chindexer.TimestampColumn + order),
		qm.Limit(limit),
	}

	optsMods, err := opts.QueryMods()
	if err != nil {
		return nil, err
	}
	mods = append(mods, optsMods...)
	query, args := newQuery(mods...)
	rows, err := s.chConn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get cloud events: %w", err)
	}

	var cloudEvents []CloudEventMetadata
	var filler string
	for rows.Next() {
		var eventMeta CloudEventMetadata
		err = rows.Scan(&eventMeta.Subject, &eventMeta.Time, &filler, &eventMeta.Source, &eventMeta.DataVersion, &eventMeta.Producer, &eventMeta.Key)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("failed to scan cloud event: %w", err)
		}
		eventMeta.Type = nameindexer.FillerToCloudType(filler)
		cloudEvents = append(cloudEvents, eventMeta)
	}
	_ = rows.Close()
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over cloud events: %w", err)
	}
	if len(cloudEvents) == 0 {
		return nil, fmt.Errorf("no cloud events found %w", sql.ErrNoRows)
	}
	return cloudEvents, nil
}

// ListCloudEvents fetches and returns the cloud events that match the given options.
func (s *Service) ListCloudEvents(ctx context.Context, bucketName string, limit int, opts SearchOptions) ([]cloudevent.CloudEvent[json.RawMessage], error) {
	return s.ListCloudEventsFromRaw(ctx, bucketName, limit, opts.ToRawSearchOptions())
}

// ListCloudEventsFromRaw fetches and returns the cloud events that match the given options.
func (s *Service) ListCloudEventsFromRaw(ctx context.Context, bucketName string, limit int, opts RawSearchOptions) ([]cloudevent.CloudEvent[json.RawMessage], error) {
	events, err := s.ListMetadataFromRaw(ctx, limit, opts)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(events))
	for i := range events {
		keys[i] = events[i].Key
	}

	data, err := s.ListCloudEventsFromKeys(ctx, keys, bucketName)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// GetLatestCloudEvent fetches and returns the latest cloud event that matches the given options.
func (s *Service) GetLatestCloudEvent(ctx context.Context, bucketName string, opts SearchOptions) (cloudevent.CloudEvent[json.RawMessage], error) {
	return s.GetLatestCloudEventFromRaw(ctx, bucketName, opts.ToRawSearchOptions())
}

// GetLatestCloudEventFromRaw fetches and returns the latest cloud event that matches the given options.
func (s *Service) GetLatestCloudEventFromRaw(ctx context.Context, bucketName string, opts RawSearchOptions) (cloudevent.CloudEvent[json.RawMessage], error) {
	key, err := s.getLatestKeyFromRaw(ctx, opts)
	if err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, err
	}

	data, err := s.GetCloudEventFromKey(ctx, key, bucketName)
	if err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, err
	}

	return data, nil
}

// ListCloudEventsFromKeys fetches and returns the cloud events for the given keys.
func (s *Service) ListCloudEventsFromKeys(ctx context.Context, keys []string, bucketName string) ([]cloudevent.CloudEvent[json.RawMessage], error) {
	data := make([]cloudevent.CloudEvent[json.RawMessage], len(keys))
	var err error
	for i, key := range keys {
		data[i], err = s.GetCloudEventFromKey(ctx, key, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to get data from key '%s': %w", key, err)
		}
	}
	return data, nil
}

// GetCloudEventFromKey fetches and returns the cloud event for the given key.
func (s *Service) GetCloudEventFromKey(ctx context.Context, key, bucketName string) (cloudevent.CloudEvent[json.RawMessage], error) {
	obj, err := s.objGetter.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer obj.Body.Close() //nolint

	data, err := io.ReadAll(obj.Body)
	if err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, fmt.Errorf("failed to read object body: %w", err)
	}
	dataObj := cloudevent.CloudEvent[json.RawMessage]{}
	if err = json.Unmarshal(data, &dataObj); err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, fmt.Errorf("failed to unmarshal data: %w", err)
	}
	return dataObj, nil
}

// GetRawObjectFromKey fetches and returns the raw object for the given key without unmarshalling to a cloud event.
func (s *Service) GetRawObjectFromKey(ctx context.Context, key, bucketName string) ([]byte, error) {
	obj, err := s.objGetter.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer obj.Body.Close() //nolint

	data, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}
	return data, nil
}

// StoreCloudEvent stores the given cloud event in S3 and ClickHouse.
func (s *Service) StoreCloudEvent(ctx context.Context, bucketName string, event cloudevent.CloudEvent[json.RawMessage]) error {
	index, err := nameindexer.CloudEventToIndex(&event.CloudEventHeader)
	if err != nil {
		return fmt.Errorf("failed to convert cloud event to index: %w", err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal cloud event: %w", err)
	}
	return s.storeObject(ctx, &index, bucketName, data)
}

// StorePartialCloudEvent stores the given cloud event in S3 and ClickHouse. Even if some parts are invalid.
func (s *Service) StorePartialCloudEvent(ctx context.Context, bucketName string, event cloudevent.CloudEvent[json.RawMessage]) error {
	index := nameindexer.CloudEventToPartialIndex(&event.CloudEventHeader)
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal cloud event: %w", err)
	}
	return s.storeObject(ctx, &index, bucketName, data)
}

// StoreObject stores the given data in S3 with the given index.
func (s *Service) storeObject(ctx context.Context, index *nameindexer.Index, bucketName string, data []byte) error {
	key, err := nameindexer.EncodeIndex(index)
	if err != nil {
		return fmt.Errorf("failed to encode index: %w", err)
	}

	_, err = s.objGetter.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to store object in S3: %w", err)
	}

	values, err := chindexer.IndexToSlice(index)
	if err != nil {
		return fmt.Errorf("failed to convert index to slice: %w", err)
	}

	err = s.chConn.Exec(ctx, chindexer.InsertStmt, values...)
	if err != nil {
		return fmt.Errorf("failed to store index in ClickHouse: %w", err)
	}

	return nil
}

// RawSearchOptions contains options for searching for indexed objects.
type RawSearchOptions struct {
	// After if set only objects after this time are returned.
	After time.Time
	// Before if set only objects before this time are returned.
	Before time.Time
	// TimestampAsc if set objects are queried and returned in ascending order by timestamp.
	// This option is not applied for the latest query.
	TimestampAsc bool
	// Type if not empty only objects with this type are returned.
	Type *string
	// DataVersion if set only objects for this data type are returned.
	DataVersion *string
	// Subject if set only objects for this subject are returned.
	Subject *string
	// Source is the party responsible for creating the data.
	Source *string
	// Producer is the specific source entity that created the data.
	Producer *string
	// Optional is the optional data for additional metadata.
	Optional *string
}

// SearchOptions contains options for searching for indexed objects.
// Utilizing strict types for the options.
type SearchOptions struct {
	// After if set only objects after this time are returned.
	After time.Time
	// Before if set only objects before this time are returned.
	Before time.Time
	// TimestampAsc if set objects are queried and returned in ascending order by timestamp.
	// This option is not applied for the latest objects query.
	TimestampAsc bool
	// Type if not empty cloudevents for this type are returned.
	Type *string
	// DataType if set only objects with a matching data version
	DataVersion *string
	// Subject if set only objects for this subject are returned.
	Subject *cloudevent.NFTDID
	// Source is the party responsible for creating the data.
	Source *common.Address
	// Producer is the specific source entity that created the data.
	Producer *cloudevent.NFTDID
	// Optional is the optional data for additional metadata.
	Optional *string
}

func (c *SearchOptions) ToRawSearchOptions() RawSearchOptions {
	if c == nil {
		return RawSearchOptions{}
	}
	opts := RawSearchOptions{
		After:        c.After,
		Before:       c.Before,
		TimestampAsc: c.TimestampAsc,
		Type:         c.Type,
		DataVersion:  c.DataVersion,
		Optional:     c.Optional,
	}
	if c.Subject != nil {
		subject := nameindexer.EncodeNFTDID(*c.Subject)
		opts.Subject = &subject
	}
	if c.Source != nil {
		source := nameindexer.EncodeAddress(*c.Source)
		opts.Source = &source
	}
	if c.Producer != nil {
		producer := nameindexer.EncodeNFTDID(*c.Producer)
		opts.Producer = &producer
	}
	return opts
}

func (o *RawSearchOptions) QueryMods() ([]qm.QueryMod, error) {
	var mods []qm.QueryMod
	if !o.After.IsZero() {
		mods = append(mods, qm.Where(chindexer.TimestampColumn+" > ?", o.After))
	}
	if !o.Before.IsZero() {
		mods = append(mods, qm.Where(chindexer.TimestampColumn+" < ?", o.Before))
	}
	if o.Type != nil {
		filler := nameindexer.CloudTypeToFiller(*o.Type)
		primaryFiller := nameindexer.EncodePrimaryFiller(filler)
		mods = append(mods, qm.Where(chindexer.PrimaryFillerColumn+" = ?", primaryFiller))
	}
	if o.DataVersion != nil {
		paddedDataType := nameindexer.EncodeDataType(*o.DataVersion)
		mods = append(mods, qm.Where(chindexer.DataTypeColumn+" = ?", paddedDataType))
	}
	if o.Subject != nil {
		subject := nameindexer.EncodeSubject(*o.Subject)
		mods = append(mods, qm.Where(chindexer.SubjectColumn+" = ?", subject))
	}
	if o.Source != nil {
		source := nameindexer.EncodeSource(*o.Source)
		mods = append(mods, qm.Where(chindexer.SourceColumn+" = ?", source))
	}
	if o.Producer != nil {
		producer := nameindexer.EncodeProducer(*o.Producer)
		mods = append(mods, qm.Where(chindexer.ProducerColumn+" = ?", producer))
	}
	if o.Optional != nil {
		mods = append(mods, qm.Where(chindexer.OptionalColumn+" = ?", *o.Optional))
	}
	return mods, nil
}

var dialect = drivers.Dialect{
	LQ: '`',
	RQ: '`',

	UseIndexPlaceholders:    false,
	UseLastInsertID:         false,
	UseSchema:               false,
	UseDefaultKeyword:       false,
	UseAutoColumns:          false,
	UseTopClause:            false,
	UseOutputClause:         false,
	UseCaseWhenExistsClause: false,
}

// newQuery initializes a new Query using the passed in QueryMods.
func newQuery(mods ...qm.QueryMod) (string, []any) {
	q := &queries.Query{}
	queries.SetDialect(q, &dialect)
	qm.Apply(q, mods...)
	return queries.BuildQuery(q)
}

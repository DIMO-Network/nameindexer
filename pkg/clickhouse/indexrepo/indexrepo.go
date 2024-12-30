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
	"github.com/volatiletech/sqlboiler/v4/drivers"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
)

// Service manages and retrieves data messages from indexed objects in S3.
type Service struct {
	objGetter ObjectGetter
	chConn    clickhouse.Conn
}

type CloudEventIndex struct {
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

// GetLatestIndex returns the latest cloud event index that matches the given options.
func (s *Service) GetLatestIndex(ctx context.Context, opts *SearchOptions) (CloudEventIndex, error) {
	opts.TimestampAsc = false
	events, err := s.ListIndexes(ctx, 1, opts)
	if err != nil {
		return CloudEventIndex{}, err
	}
	return events[0], nil
}

// ListIndexes fetches and returns a list of index for cloud events that match the given options.
func (s *Service) ListIndexes(ctx context.Context, limit int, opts *SearchOptions) ([]CloudEventIndex, error) {
	order := " DESC"
	if opts != nil && opts.TimestampAsc {
		order = " ASC"
	}
	mods := []qm.QueryMod{
		qm.Select(chindexer.SubjectColumn,
			chindexer.TimestampColumn,
			chindexer.TypeColumn,
			chindexer.IDColumn,
			chindexer.SourceColumn,
			chindexer.ProducerColumn,
			chindexer.DataContentTypeColumn,
			chindexer.DataVersionColumn,
			chindexer.ExtrasColumn,
			chindexer.IndexKeyColumn,
		),
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

	var cloudEvents []CloudEventIndex
	var extras string
	for rows.Next() {
		var eventMeta CloudEventIndex
		err = rows.Scan(&eventMeta.Subject, &eventMeta.Time, &eventMeta.Type, &eventMeta.ID, &eventMeta.Source, &eventMeta.Producer, &eventMeta.DataContentType, &eventMeta.DataVersion, &extras, &eventMeta.Key)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("failed to scan cloud event: %w", err)
		}
		if extras != "" {
			if err = json.Unmarshal([]byte(extras), &eventMeta.Extras); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("failed to unmarshal extras: %w", err)
			}
		}
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
func (s *Service) ListCloudEvents(ctx context.Context, bucketName string, limit int, opts *SearchOptions) ([]cloudevent.CloudEvent[json.RawMessage], error) {
	events, err := s.ListIndexes(ctx, limit, opts)
	if err != nil {
		return nil, err
	}
	data, err := s.ListCloudEventsFromIndexes(ctx, events, bucketName)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// GetLatestCloudEvent fetches and returns the latest cloud event that matches the given options.
func (s *Service) GetLatestCloudEvent(ctx context.Context, bucketName string, opts *SearchOptions) (cloudevent.CloudEvent[json.RawMessage], error) {
	cloudIdx, err := s.GetLatestIndex(ctx, opts)
	if err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, err
	}

	data, err := s.GetCloudEventFromIndex(ctx, cloudIdx, bucketName)
	if err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, err
	}

	return data, nil
}

// ListCloudEventsFromIndexes fetches and returns the cloud events for the given index.
func (s *Service) ListCloudEventsFromIndexes(ctx context.Context, indexes []CloudEventIndex, bucketName string) ([]cloudevent.CloudEvent[json.RawMessage], error) {
	events := make([]cloudevent.CloudEvent[json.RawMessage], len(indexes))
	var err error
	objectsByKeys := map[string][]byte{}
	for i := range indexes {
		// Some objects have multiple cloud events so we cache the objects to avoid fetching them multiple times.
		if obj, ok := objectsByKeys[indexes[i].Key]; ok {
			events[i] = cloudevent.CloudEvent[json.RawMessage]{CloudEventHeader: indexes[i].CloudEventHeader, Data: obj}
			continue
		}
		events[i], err = s.GetCloudEventFromIndex(ctx, indexes[i], bucketName)
		if err != nil {
			return nil, err
		}
		objectsByKeys[indexes[i].Key] = events[i].Data
	}
	return events, nil
}

// GetCloudEventFromIndex fetches and returns the cloud event for the given index.
func (s *Service) GetCloudEventFromIndex(ctx context.Context, index CloudEventIndex, bucketName string) (cloudevent.CloudEvent[json.RawMessage], error) {
	rawData, err := s.GetObjectFromKey(ctx, index.Key, bucketName)
	if err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, err
	}
	return cloudevent.CloudEvent[json.RawMessage]{CloudEventHeader: index.CloudEventHeader, Data: rawData}, nil
}

// ListObjectsFromKeys fetches and returns the objects for the given keys.
func (s *Service) ListObjectsFromKeys(ctx context.Context, keys []string, bucketName string) ([][]byte, error) {
	data := make([][]byte, len(keys))
	var err error
	for i, key := range keys {
		data[i], err = s.GetObjectFromKey(ctx, key, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to get data from key '%s': %w", key, err)
		}
	}
	return data, nil
}

// GetRawObjectFromKey fetches and returns the raw object for the given key without unmarshalling to a cloud event.
func (s *Service) GetObjectFromKey(ctx context.Context, key, bucketName string) ([]byte, error) {
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
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal cloud event: %w", err)
	}
	return s.storeObject(ctx, &event.CloudEventHeader, bucketName, data)
}

// StoreObject stores the given data in S3 with the given index.
func (s *Service) storeObject(ctx context.Context, cloudHeader *cloudevent.CloudEventHeader, bucketName string, data []byte) error {
	key := nameindexer.CloudEventToIndexKey(cloudHeader)
	_, err := s.objGetter.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to store object in S3: %w", err)
	}

	values := chindexer.CloudEventToSlice(cloudHeader)

	err = s.chConn.Exec(ctx, chindexer.InsertStmt, values...)
	if err != nil {
		return fmt.Errorf("failed to store index in ClickHouse: %w", err)
	}

	return nil
}

// SearchOptions contains options for searching for indexed objects.
type SearchOptions struct {
	// After if set only objects after this time are returned.
	After time.Time
	// Before if set only objects before this time are returned.
	Before time.Time
	// Subject if set only objects for this subject are returned.
	Subject *string
	// TimestampAsc if set objects are queried and returned in ascending order by timestamp.
	// This option is not applied for the latest query.
	TimestampAsc bool
	// Type if not empty only objects with this type are returned.
	Type *string
	// ID if set only objects with this ID are returned.
	ID *string
	// Source is the party responsible for creating the data.
	Source *string
	// Producer is the specific source entity that created the data.
	Producer *string
	// DataVersion if set only objects for this data type are returned.
	DataVersion *string
	// DataContentType is the type of data of this object.
	DataContentType *string
	// Extras is the extra metadata for the cloud event.
	Extras *string
	// IndexKey is the key of the backing object for this cloud event.
	IndexKey *string
}

func (o *SearchOptions) QueryMods() ([]qm.QueryMod, error) {
	if o == nil {
		return nil, nil
	}
	var mods []qm.QueryMod
	if !o.After.IsZero() {
		mods = append(mods, qm.Where(chindexer.TimestampColumn+" > ?", o.After))
	}
	if !o.Before.IsZero() {
		mods = append(mods, qm.Where(chindexer.TimestampColumn+" < ?", o.Before))
	}
	if o.Type != nil {
		mods = append(mods, qm.Where(chindexer.TypeColumn+" = ?", *o.Type))
	}
	if o.DataVersion != nil {
		mods = append(mods, qm.Where(chindexer.DataVersionColumn+" = ?", *o.DataVersion))
	}
	if o.Subject != nil {
		mods = append(mods, qm.Where(chindexer.SubjectColumn+" = ?", *o.Subject))
	}
	if o.Source != nil {
		mods = append(mods, qm.Where(chindexer.SourceColumn+" = ?", *o.Source))
	}
	if o.Producer != nil {
		mods = append(mods, qm.Where(chindexer.ProducerColumn+" = ?", *o.Producer))
	}
	if o.Extras != nil {
		mods = append(mods, qm.Where(chindexer.ExtrasColumn+" = ?", *o.Extras))
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

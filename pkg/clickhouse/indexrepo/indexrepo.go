// Package service contains service code for gettting and managing index files.
package indexrepo

import (
	"bytes"
	"context"
	"database/sql"
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

// Service manages and retrieves data messages from index files in S3.
type Service struct {
	objGetter ObjectGetter
	chConn    clickhouse.Conn
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

// GetLatestCloudEvenFileName returns the latest filename for the given subject and data type.
func (s *Service) GetLatestCloudEventFileName(ctx context.Context, opts CloudEventSearchOptions) (string, error) {
	searchOpts, err := opts.ToSearchOptions()
	if err != nil {
		return "", fmt.Errorf("failed to convert cloud event search options: %w", err)
	}
	return s.GetLatestFileName(ctx, searchOpts)
}

// GetLatestFileName returns the latest filename for the given subject and data type.
func (s *Service) GetLatestFileName(ctx context.Context, opts SearchOptions) (string, error) {
	mods := []qm.QueryMod{
		qm.Select("argMax(" + chindexer.FileNameColumn + ", " + chindexer.TimestampColumn + ") AS filename"),
		qm.From(chindexer.TableName),
	}
	optsMods, err := opts.QueryMods()
	if err != nil {
		return "", err
	}
	mods = append(mods, optsMods...)
	query, args := newQuery(mods...)
	var filename string
	err = s.chConn.QueryRow(ctx, query, args...).Scan(&filename)
	if err != nil {
		return "", fmt.Errorf("failed to get latest filename: %w", err)
	}
	if filename == "" {
		return "", fmt.Errorf("no filenames found for subject %w", sql.ErrNoRows)
	}
	return filename, nil
}

// GetCloudEventFileNames fetches and returns the filenames for the given options.
func (s *Service) GetCloudEventFileNames(ctx context.Context, limit int, opts CloudEventSearchOptions) ([]string, error) {
	searchOpts, err := opts.ToSearchOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to convert cloud event search options: %w", err)
	}
	return s.GetFileNames(ctx, limit, searchOpts)
}

// GetFileNames fetches and returns the filenames for the given options.
func (s *Service) GetFileNames(ctx context.Context, limit int, opts SearchOptions) ([]string, error) {
	order := " DESC"
	if opts.TimestampAsc {
		order = " ASC"
	}
	mods := []qm.QueryMod{
		qm.Select(chindexer.FileNameColumn),
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
		return nil, fmt.Errorf("failed to get filenames: %w", err)
	}

	var filenames []string
	for rows.Next() {
		var filename string
		err = rows.Scan(&filename)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("failed to scan filename: %w", err)
		}
		filenames = append(filenames, filename)
	}
	_ = rows.Close()
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over filenames: %w", err)
	}
	return filenames, nil
}

// GetDataFromFiles fetches and returns the data for the given filenames.
func (s *Service) GetDataFromFileNames(ctx context.Context, filenames []string, bucketName string) ([][]byte, error) {
	data := make([][]byte, len(filenames))
	var err error
	for i, filename := range filenames {
		data[i], err = s.GetDataFromFile(ctx, filename, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to get data from file: %w", err)
		}
	}
	return data, nil
}

// GetCloudEventData fetches and returns the data for the given subject.
func (s *Service) GetCloudEventData(ctx context.Context, bucketName string, limit int, opts CloudEventSearchOptions) ([][]byte, error) {
	searchOpts, err := opts.ToSearchOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to convert cloud event search options: %w", err)
	}
	return s.GetData(ctx, bucketName, limit, searchOpts)
}

// GetData fetches and returns the data for the given subject.
func (s *Service) GetData(ctx context.Context, bucketName string, limit int, opts SearchOptions) ([][]byte, error) {
	filenames, err := s.GetFileNames(ctx, limit, opts)
	if err != nil {
		return nil, err
	}

	data, err := s.GetDataFromFileNames(ctx, filenames, bucketName)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// GetLatestCloudEventData fetches and returns the latest data for the given subject.
func (s *Service) GetLatestCloudEventData(ctx context.Context, bucketName string, opts CloudEventSearchOptions) ([]byte, error) {
	searchOpts, err := opts.ToSearchOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to convert cloud event search options: %w", err)
	}
	return s.GetLatestData(ctx, bucketName, searchOpts)
}

// GetLatestData fetches and returns the latest data for the given subject.
func (s *Service) GetLatestData(ctx context.Context, bucketName string, opts SearchOptions) ([]byte, error) {
	filename, err := s.GetLatestFileName(ctx, opts)
	if err != nil {
		return nil, err
	}

	data, err := s.GetDataFromFile(ctx, filename, bucketName)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// GetDataFromFile gets the data from S3 by filename.
func (s *Service) GetDataFromFile(ctx context.Context, filename, bucketName string) ([]byte, error) {
	obj, err := s.objGetter.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(filename),
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

// StoreCloudEventFile stores the given data in S3 with the given index.
func (s *Service) StoreCloudEventFile(ctx context.Context, cloudIndex *nameindexer.CloudEventIndex, bucketName string, data []byte) error {
	index, err := cloudIndex.ToIndex()
	if err != nil {
		return fmt.Errorf("failed to convert cloud event index to index: %w", err)
	}
	return s.StoreFile(ctx, &index, bucketName, data)
}

// StoreFile stores the given data in S3 with the given index.
func (s *Service) StoreFile(ctx context.Context, index *nameindexer.Index, bucketName string, data []byte) error {
	fileName, err := nameindexer.EncodeIndex(index)
	if err != nil {
		return fmt.Errorf("failed to encode index: %w", err)
	}

	_, err = s.objGetter.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &fileName,
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

type SearchOptions struct {
	// After if set only files after this time are returned.
	After time.Time
	// Before if set only files before this time are returned.
	Before time.Time
	// TimestampAsc if set files are queried and returned in ascending order by timestamp.
	// This option is not applied for the latest file query.
	TimestampAsc bool
	// PrimaryFiller if set only files for this primary filler are returned.
	PrimaryFiller *string
	// DataType if set only files for this data type are returned.
	DataType *string
	// Subject if set only files for this subject are returned.
	Subject *string
	// SecondaryFiller if set only files for this secondary filler are returned.
	SecondaryFiller *string
	// Source is the party responsible for creating the data.
	Source *string
	// Producer is the specific source entity that created the data.
	Producer *string
	// Optional is the optional data for additional metadata.
	Optional *string
}

type CloudEventSearchOptions struct {
	// After if set only files after this time are returned.
	After time.Time
	// Before if set only files before this time are returned.
	Before time.Time
	// TimestampAsc if set files are queried and returned in ascending order by timestamp.
	// This option is not applied for the latest file query.
	TimestampAsc bool
	// PrimaryFiller if set only files for this primary filler are returned.
	PrimaryFiller *string
	// DataType if set only files for this data type are returned.
	DataType *string
	// Subject if set only files for this subject are returned.
	Subject *cloudevent.NFTDID
	// SecondaryFiller if set only files for this secondary filler are returned.
	SecondaryFiller *string
	// Source is the party responsible for creating the data.
	Source *common.Address
	// Producer is the specific source entity that created the data.
	Producer *cloudevent.NFTDID
	// Optional is the optional data for additional metadata.
	Optional *string
}

func (c *CloudEventSearchOptions) ToSearchOptions() (SearchOptions, error) {
	opts := SearchOptions{
		After:           c.After,
		Before:          c.Before,
		TimestampAsc:    c.TimestampAsc,
		PrimaryFiller:   c.PrimaryFiller,
		DataType:        c.DataType,
		SecondaryFiller: c.SecondaryFiller,
		Optional:        c.Optional,
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
	return opts, nil
}

func (o *SearchOptions) QueryMods() ([]qm.QueryMod, error) {
	var mods []qm.QueryMod
	if !o.After.IsZero() {
		mods = append(mods, qm.Where(chindexer.TimestampColumn+" > ?", o.After))
	}
	if !o.Before.IsZero() {
		mods = append(mods, qm.Where(chindexer.TimestampColumn+" < ?", o.Before))
	}
	if o.PrimaryFiller != nil {
		primaryFiller := nameindexer.EncodePrimaryFiller(*o.PrimaryFiller)
		mods = append(mods, qm.Where(chindexer.PrimaryFillerColumn+" = ?", primaryFiller))
	}
	if o.DataType != nil {
		paddedDataType := nameindexer.EncodeDataType(*o.DataType)
		mods = append(mods, qm.Where(chindexer.DataTypeColumn+" = ?", paddedDataType))
	}
	if o.Subject != nil {
		subject := nameindexer.EncodeSubject(*o.Subject)
		mods = append(mods, qm.Where(chindexer.SubjectColumn+" = ?", subject))
	}
	if o.SecondaryFiller != nil {
		secondaryFiller := nameindexer.EncodeSecondaryFiller(*o.SecondaryFiller)
		mods = append(mods, qm.Where(chindexer.SecondaryFillerColumn+" = ?", secondaryFiller))
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

// Package service contains service code for gettting and managing index files.
//
//go:generate mockgen -source=./service.go -destination=service_mock_test.go -package=service_test
package service

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/nameindexer"
	chindexer "github.com/DIMO-Network/nameindexer/pkg/clickhouse"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Service manages and retrieves data messages from index files in S3.
type Service struct {
	objGetter  ObjectGetter
	chConn     clickhouse.Conn
	bucketName string
}

// ObjectGetter is an interface for getting an object from S3.
type ObjectGetter interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// New creates a new instance of serviceService.
func New(chConn clickhouse.Conn, objGetter ObjectGetter, bucketName string) *Service {
	return &Service{
		objGetter:  objGetter,
		chConn:     chConn,
		bucketName: bucketName,
	}
}

// GetLatestFileName returns the latest filename for the given subject and data type.
func (s *Service) GetLatestFileName(ctx context.Context, dataType string, subject nameindexer.Subject) (string, error) {
	paddedDataType, err := nameindexer.SantatizeDataType(dataType)
	if err != nil {
		return "", fmt.Errorf("failed to sanitize data type: %w", err)
	}
	query := fmt.Sprintf("SELECT argMax(%s, %s) AS filename FROM %s WHERE %s = ? AND %s = ?", chindexer.FileNameColumn, chindexer.TimestampColumn, chindexer.TableName, chindexer.SubjectColumn, chindexer.DataTypeColumn)
	var filename string
	err = s.chConn.QueryRow(ctx, query, subject, paddedDataType).Scan(&filename)
	if err != nil {
		return "", fmt.Errorf("failed to get latest filename: %w", err)
	}
	if filename == "" {
		return "", fmt.Errorf("no filenames found for subject %w", sql.ErrNoRows)
	}
	return filename, nil
}

// GetLatestData fetches and returns the latest data for the given subject.
func (s *Service) GetLatestData(ctx context.Context, dataType string, subject nameindexer.Subject) ([]byte, error) {
	filename, err := s.GetLatestFileName(ctx, dataType, subject)
	if err != nil {
		return nil, err
	}

	data, err := s.GetDataFromFile(ctx, filename)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// GetDataFromFile gets the data from S3 by filename.
func (s *Service) GetDataFromFile(ctx context.Context, filename string) ([]byte, error) {
	obj, err := s.objGetter.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
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

// StoreFile stores the given data in S3 with the given index.
func (s *Service) StoreFile(ctx context.Context, index *nameindexer.Index, data []byte) error {
	fileName, err := nameindexer.EncodeIndex(index)
	if err != nil {
		return fmt.Errorf("failed to encode index: %w", err)
	}

	_, err = s.objGetter.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucketName,
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

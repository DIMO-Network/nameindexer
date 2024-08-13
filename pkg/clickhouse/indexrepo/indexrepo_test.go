//go:generate mockgen -source=./indexrepo.go -destination=indexrepo_mock_test.go -package=indexrepo_test
package indexrepo_test

import (
	"bytes"
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chconfig "github.com/DIMO-Network/clickhouse-infra/pkg/connect/config"
	"github.com/DIMO-Network/clickhouse-infra/pkg/container"
	"github.com/DIMO-Network/nameindexer"
	chindexer "github.com/DIMO-Network/nameindexer/pkg/clickhouse"
	"github.com/DIMO-Network/nameindexer/pkg/clickhouse/indexrepo"
	"github.com/DIMO-Network/nameindexer/pkg/clickhouse/migrations"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var dataType = "small"

// setupClickHouseContainer starts a ClickHouse container for testing and returns the connection.
func setupClickHouseContainer(t *testing.T) *container.Container {
	ctx := context.Background()
	settings := chconfig.Settings{
		User:     "default",
		Database: "dimo",
	}

	chContainer, err := container.CreateClickHouseContainer(ctx, settings)
	require.NoError(t, err)

	chDB, err := chContainer.GetClickhouseAsDB()
	require.NoError(t, err)

	// Ensure we terminate the container at the end
	t.Cleanup(func() {
		chContainer.Terminate(ctx)
	})

	err = migrations.RunGoose(ctx, []string{"up"}, chDB)
	require.NoError(t, err)

	return chContainer
}

// insertTestData inserts test data into ClickHouse.
func insertTestData(t *testing.T, ctx context.Context, conn clickhouse.Conn, index nameindexer.Index) string {
	values, err := chindexer.IndexToSlice(&index)
	require.NoError(t, err)

	err = conn.Exec(ctx, chindexer.InsertStmt, values...)
	require.NoError(t, err)
	filename, err := nameindexer.EncodeIndex(&index)
	require.NoError(t, err)
	return filename
}

// TestGetLatestFileName tests the GetLatestFileName function.
func TestGetLatestFileName(t *testing.T) {
	chContainer := setupClickHouseContainer(t)

	// Insert test data
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	deviceAddr1 := randAddress()
	deviceAddr2 := randAddress()
	tokenID := uint32(1234567890)
	imei := "123456789012345"
	ctx := context.Background()
	now := time.Now()
	_ = insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)}, DataType: dataType, Timestamp: now.Add(-1 * time.Hour)})
	file2Name := insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)}, DataType: dataType, Timestamp: now})
	tokenIDFileName := insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.TokenID(tokenID)}, DataType: dataType, Timestamp: now})
	imeiFileName := insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.IMEI(imei)}, DataType: dataType, Timestamp: now})

	tests := []struct {
		name          string
		subject       nameindexer.Subject
		expectedFile  string
		expectedError bool
	}{
		{
			name:         "valid latest file",
			subject:      nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)},
			expectedFile: file2Name,
		},
		{
			name:          "no records",
			subject:       nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr2)},
			expectedError: true,
		},
		{
			name:         "valid latest file with token ID",
			subject:      nameindexer.Subject{Identifier: nameindexer.TokenID(tokenID)},
			expectedFile: tokenIDFileName,
		},
		{
			name:         "valid latest file with IMEI",
			subject:      nameindexer.Subject{Identifier: nameindexer.IMEI(imei)},
			expectedFile: imeiFileName,
		},
	}

	indexFileService := indexrepo.New(conn, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := indexrepo.SearchOptions{
				DataType: &dataType,
				Subject:  &tt.subject,
			}
			filename, err := indexFileService.GetLatestFileName(context.Background(), opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedFile, filename)
			}
		})
	}
}

// TestGetDataFromFile tests the GetDataFromFile function.
func TestGetDataFromFile(t *testing.T) {
	chContainer := setupClickHouseContainer(t)
	deviceAddr1 := randAddress()
	deviceAddr2 := randAddress()

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()
	_ = insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)}, DataType: dataType, Timestamp: time.Now().Add(-1 * time.Hour)})

	tests := []struct {
		name            string
		deviceAddr      common.Address
		expectedContent []byte
		expectedError   bool
	}{
		{
			name:            "valid file content",
			deviceAddr:      deviceAddr1,
			expectedContent: []byte(`{"vin": "1HGCM82633A123456"}`),
		},
		{
			name:          "no records",
			deviceAddr:    deviceAddr2,
			expectedError: true,
		},
	}

	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)
	content := []byte(`{"vin": "1HGCM82633A123456"}`)
	mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(content)),
		ContentLength: ref(int64(len(content))),
	}, nil).AnyTimes()

	indexFileService := indexrepo.New(conn, mockS3Client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := indexrepo.SearchOptions{
				DataType: &dataType,
				Subject:  &nameindexer.Subject{Identifier: nameindexer.Address(tt.deviceAddr)},
			}
			content, err := indexFileService.GetLatestData(context.Background(), "test-bucket", opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedContent, content)
			}
		})
	}
}

// TestStoreFile tests the StoreFile function.
func TestStoreFile(t *testing.T) {
	chContainer := setupClickHouseContainer(t)
	deviceAddr1 := randAddress()

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)
	mockS3Client.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.PutObjectOutput{}, nil).AnyTimes()

	indexFileService := indexrepo.New(conn, mockS3Client)

	content := []byte(`{"vin": "1HGCM82633A123456"}`)
	index := nameindexer.Index{
		Subject:   nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)},
		DataType:  dataType,
		Timestamp: time.Now(),
	}

	err = indexFileService.StoreFile(ctx, &index, "test-bucket", content)
	require.NoError(t, err)

	// Verify the data is stored in ClickHouse
	opts := indexrepo.SearchOptions{
		DataType: &dataType,
		Subject:  &nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)},
	}
	filename, err := indexFileService.GetLatestFileName(ctx, opts)
	require.NoError(t, err)
	expectedFileName, err := nameindexer.EncodeIndex(&index)
	require.NoError(t, err)
	require.Equal(t, expectedFileName, filename)
}

// TestGetData tests the GetData function with different SearchOptions combinations.
func TestGetData(t *testing.T) {
	chContainer := setupClickHouseContainer(t)

	// Insert test data
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	deviceAddr1 := randAddress()
	deviceAddr2 := randAddress()
	tokenID := uint32(1234567890)
	imei := "123456789012345"
	ctx := context.Background()
	now := time.Now()
	file1Name := insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)}, DataType: dataType, Timestamp: now.Add(-4 * time.Hour)})
	file2Name := insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)}, DataType: dataType, Timestamp: now.Add(-3 * time.Hour)})
	file3Name := insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)}, DataType: dataType, Timestamp: now.Add(-2 * time.Hour), PrimaryFiller: "AA"})
	file4Name := insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)}, DataType: dataType, Timestamp: now.Add(-1 * time.Hour), SecondaryFiller: "55"})
	tokenIDFileName := insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.TokenID(tokenID)}, DataType: dataType, Timestamp: now.Add(-2 * time.Minute)})
	imeiFileName := insertTestData(t, ctx, conn, nameindexer.Index{Subject: nameindexer.Subject{Identifier: nameindexer.IMEI(imei)}, DataType: dataType, Timestamp: now.Add(-1 * time.Minute)})

	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)

	indexFileService := indexrepo.New(conn, mockS3Client)

	tests := []struct {
		name          string
		opts          indexrepo.SearchOptions
		expectedFiles []string
		expectedError bool
	}{
		{
			name: "valid data with address",
			opts: indexrepo.SearchOptions{
				DataType: &dataType,
				Subject:  &nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)},
			},
			expectedFiles: []string{file1Name, file2Name, file3Name, file4Name},
		},
		{
			name: "no records with address",
			opts: indexrepo.SearchOptions{
				DataType: &dataType,
				Subject:  &nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr2)},
			},
			expectedFiles: nil,
		},
		{
			name: "valid data with token ID",
			opts: indexrepo.SearchOptions{
				DataType: &dataType,
				Subject:  &nameindexer.Subject{Identifier: nameindexer.TokenID(tokenID)},
			},
			expectedFiles: []string{tokenIDFileName},
		},
		{
			name: "valid data with IMEI",
			opts: indexrepo.SearchOptions{
				DataType: &dataType,
				Subject:  &nameindexer.Subject{Identifier: nameindexer.IMEI(imei)},
			},
			expectedFiles: []string{imeiFileName},
		},
		{
			name: "data within time range",
			opts: indexrepo.SearchOptions{
				DataType: &dataType,
				After:    now.Add(-3 * time.Hour),
				Before:   now.Add(-1 * time.Minute),
			},
			expectedFiles: []string{file3Name, file4Name, tokenIDFileName},
		},
		{
			name: "data with primary filler",
			opts: indexrepo.SearchOptions{
				DataType:      &dataType,
				PrimaryFiller: ref("MM"),
			},
			expectedFiles: []string{file1Name, file2Name, file4Name, tokenIDFileName, imeiFileName},
		},
		{
			name: "data with secondary filler",
			opts: indexrepo.SearchOptions{
				DataType:        &dataType,
				SecondaryFiller: ref("00"),
			},
			expectedFiles: []string{file1Name, file2Name, file3Name, tokenIDFileName, imeiFileName},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var expectedContent [][]byte
			for _, fileName := range tt.expectedFiles {
				mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					require.Equal(t, *params.Key, fileName)
					content := []byte(`{"data": {"` + fileName + `"}}`)
					expectedContent = append(expectedContent, content)
					return &s3.GetObjectOutput{
						Body:          io.NopCloser(bytes.NewReader(content)),
						ContentLength: ref(int64(len(content))),
					}, nil
				})
			}
			data, err := indexFileService.GetData(context.Background(), "test-bucket", 10, tt.opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.Len(t, data, len(expectedContent))
				for i, content := range expectedContent {
					require.Equal(t, content, data[i])
				}
				require.NoError(t, err)
			}
		})
	}
}

func ref[T any](x T) *T {
	return &x
}

func randAddress() common.Address {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		log.Fatalf("Failed to generate private key: %v", err)
	}
	return crypto.PubkeyToAddress(privateKey.PublicKey)
}

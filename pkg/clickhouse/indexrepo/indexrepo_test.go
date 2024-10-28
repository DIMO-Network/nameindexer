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
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
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
func insertTestData(t *testing.T, ctx context.Context, conn clickhouse.Conn, index *nameindexer.CloudEventIndex) string {
	values, err := chindexer.CloudEventIndexToSlice(index)
	require.NoError(t, err)

	err = conn.Exec(ctx, chindexer.InsertStmt, values...)
	require.NoError(t, err)
	filename, err := nameindexer.EncodeCloudEventIndex(index)
	require.NoError(t, err)
	return filename
}

// TestGetLatestFileName tests the GetLatestFileName function.
func TestGetLatestFileName(t *testing.T) {
	chContainer := setupClickHouseContainer(t)

	// Insert test data
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	contractAddr := randAddress()
	device1TokenID := uint32(1234567890)
	device2TokenID := uint32(976543210)
	ctx := context.Background()
	now := time.Now()

	// Create test indices
	eventIdx1 := &nameindexer.CloudEventIndex{
		Subject: cloudevent.NFTDID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		},
		DataType:  dataType,
		Timestamp: now.Add(-1 * time.Hour),
	}

	eventIdx2 := &nameindexer.CloudEventIndex{
		Subject: cloudevent.NFTDID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		},
		DataType:  dataType,
		Timestamp: now,
	}

	// Insert test data
	_ = insertTestData(t, ctx, conn, eventIdx1)
	file2Name := insertTestData(t, ctx, conn, eventIdx2)

	tests := []struct {
		name          string
		subject       cloudevent.NFTDID
		expectedFile  string
		expectedError bool
	}{
		{
			name: "valid latest file",
			subject: cloudevent.NFTDID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device1TokenID,
			},
			expectedFile: file2Name,
		},
		{
			name: "no records",
			subject: cloudevent.NFTDID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device2TokenID,
			},
			expectedError: true,
		},
	}

	indexFileService := indexrepo.New(conn, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := indexrepo.CloudEventSearchOptions{
				DataType: &dataType,
				Subject:  &tt.subject,
			}
			filename, err := indexFileService.GetLatestCloudEventFileName(context.Background(), opts)

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
	contractAddr := randAddress()
	device1TokenID := uint32(1234567890)
	device2TokenID := uint32(976543210)

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()

	eventIdx := &nameindexer.CloudEventIndex{
		Subject: cloudevent.NFTDID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		},
		DataType:  dataType,
		Timestamp: time.Now().Add(-1 * time.Hour),
	}

	_ = insertTestData(t, ctx, conn, eventIdx)

	tests := []struct {
		name            string
		subject         cloudevent.NFTDID
		expectedContent []byte
		expectedError   bool
	}{
		{
			name: "valid file content",
			subject: cloudevent.NFTDID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device1TokenID,
			},
			expectedContent: []byte(`{"vin": "1HGCM82633A123456"}`),
		},
		{
			name: "no records",
			subject: cloudevent.NFTDID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device2TokenID,
			},
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
			opts := indexrepo.CloudEventSearchOptions{
				DataType: &dataType,
				Subject:  &tt.subject,
			}
			content, err := indexFileService.GetLatestCloudEventData(context.Background(), "test-bucket", opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedContent, content)
			}
		})
	}
}

func TestStoreFile(t *testing.T) {
	chContainer := setupClickHouseContainer(t)

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)
	mockS3Client.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.PutObjectOutput{}, nil).AnyTimes()

	indexFileService := indexrepo.New(conn, mockS3Client)

	content := []byte(`{"vin": "1HGCM82633A123456"}`)
	index := nameindexer.CloudEventIndex{
		Subject: cloudevent.NFTDID{
			ChainID:         153,
			ContractAddress: randAddress(),
			TokenID:         123456,
		},
		DataType:  dataType,
		Timestamp: time.Now(),
	}

	err = indexFileService.StoreCloudEventFile(ctx, &index, "test-bucket", content)
	require.NoError(t, err)

	// Verify the data is stored in ClickHouse
	opts := indexrepo.CloudEventSearchOptions{
		DataType: &dataType,
		Subject:  &index.Subject,
	}
	filename, err := indexFileService.GetLatestCloudEventFileName(ctx, opts)
	require.NoError(t, err)
	expectedFileName, err := nameindexer.EncodeCloudEventIndex(&index)
	require.NoError(t, err)
	require.Equal(t, expectedFileName, filename)
}

// TestGetData tests the GetData function with different SearchOptions combinations.
func TestGetData(t *testing.T) {
	chContainer := setupClickHouseContainer(t)

	// Insert test data
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	source1 := randAddress()
	contractAddr := randAddress()
	device1TokenID := uint32(123456)
	device2TokenID := uint32(654321)
	ctx := context.Background()
	now := time.Now()

	eventIdx := nameindexer.CloudEventIndex{
		Subject: cloudevent.NFTDID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		},
		Timestamp: now.Add(-4 * time.Hour),
		Producer: cloudevent.NFTDID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		},
		PrimaryFiller:   "0S",
		Source:          source1,
		DataType:        dataType,
		SecondaryFiller: "00",
	}
	file1Name := insertTestData(t, ctx, conn, &eventIdx)
	eventIdx2 := eventIdx
	eventIdx2.Timestamp = now.Add(-3 * time.Hour)
	file2Name := insertTestData(t, ctx, conn, &eventIdx2)
	eventIdx3 := eventIdx
	eventIdx3.Timestamp = now.Add(-2 * time.Hour)
	eventIdx3.PrimaryFiller = "0F"
	file3Name := insertTestData(t, ctx, conn, &eventIdx3)
	eventIdx4 := eventIdx
	eventIdx4.Timestamp = now.Add(-1 * time.Hour)
	eventIdx4.SecondaryFiller = "55"
	file4Name := insertTestData(t, ctx, conn, &eventIdx4)

	tests := []struct {
		name          string
		opts          indexrepo.CloudEventSearchOptions
		expectedFiles []string
		expectedError bool
	}{
		{
			name: "valid data with address",
			opts: indexrepo.CloudEventSearchOptions{
				DataType: &dataType,
				Subject:  &eventIdx.Subject,
			},
			expectedFiles: []string{file4Name, file3Name, file2Name, file1Name},
		},
		{
			name: "no records with address",
			opts: indexrepo.CloudEventSearchOptions{
				DataType: &dataType,
				Subject: &cloudevent.NFTDID{
					ChainID:         153,
					ContractAddress: contractAddr,
					TokenID:         device2TokenID,
				},
			},
			expectedFiles: nil,
			expectedError: true,
		},
		{
			name: "data within time range",
			opts: indexrepo.CloudEventSearchOptions{
				DataType: &dataType,
				After:    now.Add(-3 * time.Hour),
				Before:   now.Add(-1 * time.Minute),
			},
			expectedFiles: []string{file4Name, file3Name},
		},
		{
			name: "data with primary filler",
			opts: indexrepo.CloudEventSearchOptions{
				DataType:      &dataType,
				PrimaryFiller: ref("0S"),
			},
			expectedFiles: []string{file4Name, file2Name, file1Name},
		},
		{
			name: "data with secondary filler",
			opts: indexrepo.CloudEventSearchOptions{
				DataType:        &dataType,
				SecondaryFiller: ref("00"),
			},
			expectedFiles: []string{file3Name, file2Name, file1Name},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockS3Client := NewMockObjectGetter(ctrl)

			indexFileService := indexrepo.New(conn, mockS3Client)
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
			data, err := indexFileService.GetCloudEventData(context.Background(), "test-bucket", 10, tt.opts)

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

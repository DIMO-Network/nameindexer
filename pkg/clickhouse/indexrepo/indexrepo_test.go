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
func insertTestData(t *testing.T, ctx context.Context, conn clickhouse.Conn, subject nameindexer.Subject, timestamp time.Time) string {
	idx := nameindexer.Index{
		Subject:   subject,
		DataType:  dataType,
		Timestamp: timestamp,
	}
	values, err := chindexer.IndexToSlice(&idx)
	require.NoError(t, err)

	err = conn.Exec(ctx, chindexer.InsertStmt, values...)
	require.NoError(t, err)
	filename, err := nameindexer.EncodeIndex(&idx)
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
	_ = insertTestData(t, ctx, conn, nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)},
		time.Now().Add(-1*time.Hour))
	file2Name := insertTestData(t, ctx, conn, nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)}, time.Now())
	tokenIDFileName := insertTestData(t, ctx, conn, nameindexer.Subject{Identifier: nameindexer.TokenID(tokenID)}, time.Now())
	imeiFileName := insertTestData(t, ctx, conn, nameindexer.Subject{Identifier: nameindexer.IMEI(imei)}, time.Now())
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

	indexFileService := indexrepo.New(conn, nil, "test-bucket")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename, err := indexFileService.GetLatestFileName(context.Background(), dataType, tt.subject)

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
	_ = insertTestData(t, ctx, conn, nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)}, time.Now().Add(-1*time.Hour))

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

	indexFileService := indexrepo.New(conn, mockS3Client, "test-bucket")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content, err := indexFileService.GetLatestData(context.Background(), dataType, nameindexer.Subject{
				Identifier: nameindexer.Address(tt.deviceAddr),
			})

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

	indexFileService := indexrepo.New(conn, mockS3Client, "test-bucket")

	content := []byte(`{"vin": "1HGCM82633A123456"}`)
	index := nameindexer.Index{
		Subject:   nameindexer.Subject{Identifier: nameindexer.Address(deviceAddr1)},
		DataType:  dataType,
		Timestamp: time.Now(),
	}

	err = indexFileService.StoreFile(ctx, &index, content)
	require.NoError(t, err)

	// Verify the data is stored in ClickHouse
	filename, err := indexFileService.GetLatestFileName(ctx, dataType, nameindexer.Subject{
		Identifier: nameindexer.Address(deviceAddr1),
	})
	require.NoError(t, err)
	expectedFileName, err := nameindexer.EncodeIndex(&index)
	require.NoError(t, err)
	require.Equal(t, expectedFileName, filename)
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

package migrations_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/clickhouse-infra/pkg/connect/config"
	"github.com/DIMO-Network/clickhouse-infra/pkg/container"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/nameindexer"
	localch "github.com/DIMO-Network/nameindexer/pkg/clickhouse"
	"github.com/DIMO-Network/nameindexer/pkg/clickhouse/migrations"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigration(t *testing.T) {
	ctx := context.Background()
	chcontainer, err := container.CreateClickHouseContainer(ctx, config.Settings{})
	require.NoError(t, err, "Failed to create clickhouse container")

	defer chcontainer.Terminate(ctx)

	db, err := chcontainer.GetClickhouseAsDB()
	require.NoError(t, err, "Failed to get clickhouse db")

	err = migrations.RunGoose(ctx, []string{"up-to", "2", "-v"}, db)
	require.NoError(t, err, "Failed to run migration")

	conn, err := chcontainer.GetClickHouseAsConn()
	require.NoError(t, err, "Failed to get clickhouse connection")

	oldIdx := &nameindexer.Index{
		Timestamp:       time.Now(),
		PrimaryFiller:   "0S",
		DataType:        "Stat/2.0.0",
		Subject:         "T000000000000000000000000000000000000003",
		SecondaryFiller: "00",
	}
	err = insesrtOldIndex(conn, oldIdx)
	require.NoError(t, err, "Failed to insert old index")

	err = migrations.RunGoose(ctx, []string{"up", "-v"}, db)
	require.NoError(t, err, "Failed to run migration")
	hdr := cloudevent.CloudEventHeader{
		Subject:     cloudevent.NFTDID{ChainID: 2, ContractAddress: common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"), TokenID: 3}.String(),
		Time:        time.Now(),
		Type:        cloudevent.TypeStatus,
		Source:      common.HexToAddress("0xb57d6d57fca59d0517038c968a1b831b071fa679").String(),
		DataVersion: "Stat/2.0.0",
		Producer:    cloudevent.NFTDID{ChainID: 3, ContractAddress: common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"), TokenID: 3}.String(),
	}
	newIdx, err := nameindexer.CloudEventToIndex(&hdr)
	require.NoError(t, err, "Failed to convert cloud event to index")
	err = insertIndex(conn, &newIdx)
	require.NoError(t, err, "Failed to insert new index")

	// Iterate over the rows and check the column names
	cols, err := getOrderByCols(ctx, conn, localch.TableName)
	require.NoError(t, err, "Failed to get current columns")
	expectedCols := []string{
		localch.SubjectColumn,
		localch.TimestampColumn,
		localch.PrimaryFillerColumn,
		localch.SourceColumn,
		localch.DataTypeColumn,
		localch.SecondaryFillerColumn,
		localch.ProducerColumn,
	}
	assert.ElementsMatch(t, expectedCols, cols, "Columns do not match")
	// Close the DB connection
	err = db.Close()
	assert.NoError(t, err, "Failed to close DB connection")
	err = conn.Close()
	assert.NoError(t, err, "Failed to close clickhouse connection")
}

func getOrderByCols(ctx context.Context, conn clickhouse.Conn, tableName string) ([]string, error) {
	selectStm := "SELECT sorting_key FROM system.tables WHERE name = ?;"
	row := conn.QueryRow(ctx, selectStm, tableName)
	var sortingKey string
	err := row.Scan(&sortingKey)
	if err != nil {
		return nil, err
	}
	return strings.Split(sortingKey, ", "), nil
}
func insesrtOldIndex(conn clickhouse.Conn, index *nameindexer.Index) error {
	oldInsertStmt := "INSERT INTO " + localch.TableName + " (" + localch.TimestampColumn + ", " + localch.PrimaryFillerColumn + ", " + localch.DataTypeColumn + ", " + localch.SubjectColumn + ", " + localch.SecondaryFillerColumn + ", " + "file_name" + ") VALUES (?, ?, ?, ?, ?, ?)"
	indexKey, err := nameindexer.EncodeIndex(index)
	if err != nil {
		return fmt.Errorf("failed to encode index: %w", err)
	}
	err = conn.Exec(context.Background(), oldInsertStmt, index.Timestamp, index.PrimaryFiller, index.DataType, index.Subject, index.SecondaryFiller, indexKey)
	if err != nil {
		return fmt.Errorf("failed to insert old index: %w", err)
	}
	return nil
}

func insertIndex(conn clickhouse.Conn, index *nameindexer.Index) error {
	values, err := localch.IndexToSlice(index)
	if err != nil {
		return fmt.Errorf("failed to convert index to slice: %w", err)
	}

	err = conn.Exec(context.Background(), localch.InsertStmt, values...)
	if err != nil {
		return fmt.Errorf("failed to store index in ClickHouse: %w", err)
	}
	return nil
}

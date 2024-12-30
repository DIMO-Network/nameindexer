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

	conn, err := chcontainer.GetClickHouseAsConn()
	require.NoError(t, err, "Failed to get clickhouse connection")

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
	cols, err := GetTableCols(ctx, conn, localch.TableName)
	require.NoError(t, err, "Failed to get Cols columns")
	expectedCols := []string{
		localch.SubjectColumn,
		localch.TimestampColumn,
		localch.TypeColumn,
		localch.IDColumn,
		localch.SourceColumn,
		localch.ProducerColumn,
		localch.DataContentTypeColumn,
		localch.DataVersionColumn,
		localch.ExtrasColumn,
		localch.IndexKeyColumn,
	}
	assert.ElementsMatch(t, expectedCols, cols, "Columns do not match")

	// Check the order of the columns
	orderByCols, err := getOrderByCols(ctx, conn, localch.TableName)
	require.NoError(t, err, "Failed to get order by columns")
	expectedOrderByCols := []string{
		localch.SubjectColumn,
		localch.TimestampColumn,
		localch.TypeColumn,
	}
	assert.ElementsMatch(t, expectedOrderByCols, orderByCols, "Order by columns do not match")
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

func GetTableCols(ctx context.Context, chConn clickhouse.Conn, tableName string) ([]string, error) {
	selectStm := fmt.Sprintf("SELECT name FROM system.columns where table='%s'", tableName)
	rows, err := chConn.Query(ctx, selectStm)
	if err != nil {
		return nil, fmt.Errorf("failed to show table: %w", err)
	}
	defer rows.Close() //nolint // we are not interested in the error here
	colInfos := []string{}
	count := 0
	for rows.Next() {
		count++
		var info string
		err := rows.Scan(&info)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		colInfos = append(colInfos, info)
	}
	return colInfos, nil
}

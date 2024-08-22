package migrations_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/clickhouse-infra/pkg/connect/config"
	"github.com/DIMO-Network/clickhouse-infra/pkg/container"
	localch "github.com/DIMO-Network/nameindexer/pkg/clickhouse"
	"github.com/DIMO-Network/nameindexer/pkg/clickhouse/migrations"
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

	err = migrations.RunGoose(ctx, []string{"up", "-v"}, db)
	require.NoError(t, err, "Failed to run migration")

	conn, err := chcontainer.GetClickHouseAsConn()
	require.NoError(t, err, "Failed to get clickhouse connection")

	// Iterate over the rows and check the column names
	cols, err := getOrderByCols(ctx, conn, localch.TableName)
	require.NoError(t, err, "Failed to get current columns")
	expectedCols := []string{
		localch.TimestampColumn,
		localch.PrimaryFillerColumn,
		localch.DataTypeColumn,
		localch.SubjectColumn,
		localch.SecondaryFillerColumn,
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

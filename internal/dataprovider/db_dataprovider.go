package dataprovider

import (
	"database/sql"
	"fmt"

	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/shared"
)

type DBDataProvider struct {
	baseDataProvider
	Connection *sql.DB
}

func NewDBDataProvider(connection *sql.DB, table *model.Table) *DBDataProvider {
	return &DBDataProvider{
		baseDataProvider: newBaseDataProvider(table),
		Connection:       connection,
	}
}

func (dataProvider *DBDataProvider) getTableSelectQuery() string {
	return fmt.Sprintf("SELECT * FROM %s.%s", dataProvider.Table.Schema, dataProvider.Table.GetFqName())
}

func (dataProvider *DBDataProvider) getTableCountQuery() string {
	return fmt.Sprintf("SELECT count(*) FROM (%s) as query", dataProvider.getTableSelectQuery())
}

func (dataProvider *DBDataProvider) GetTotalCount() (int, error) {
	query := dataProvider.getTableCountQuery()

	row := dataProvider.Connection.QueryRow(query)

	var count int
	err := row.Scan(&count)

	if err != nil {
		return 0, fmt.Errorf("failed to get count: %w", err)
	}

	return count, nil
}

func (dataProvider *DBDataProvider) FetchData() (map[string]shared.Record, error) {
	query := dataProvider.getTableSelectQuery()

	rows, err := dataProvider.Connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	records := make(map[string]shared.Record)

	for rows.Next() {
		record, err := dataProvider.fetchRecord(rows)
		if err != nil {
			return nil, err
		}

		modelRecord := dataProvider.FilterRecordByModelColumns(record)
		keyValue := dataProvider.GetRecordIdentifier(modelRecord)

		records[keyValue] = modelRecord
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	return records, nil
}

func (dataProvider *DBDataProvider) fetchRecord(rows *sql.Rows) (shared.Record, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	recordValues := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(recordValues))
	for i := range recordValues {
		scanArgs[i] = &recordValues[i]
	}

	err = rows.Scan(scanArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	record := make(shared.Record)
	for i, colName := range columns {
		record[colName] = recordValues[i]
	}

	return record, nil
}

package dataprovider

import (
	"database/sql"
	"fmt"

	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/shared"
)

type DataProvider interface {
	GetTotalCount() (int, error)
	FetchData() (map[string]shared.Record, error)
}

type DBDataProvider struct {
	Connection *sql.DB
	Table      *model.Table
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

	columns, err := rows.Columns()

	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	records := make(map[string]shared.Record)

	keyName := dataProvider.Table.GetKeys()[0].Name

	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		record := make(shared.Record)

		for i, colName := range columns {
			val := values[i]
			record[colName] = val
		}

		records[fmt.Sprintf("%v", record[keyName])] = record
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	return records, nil
}

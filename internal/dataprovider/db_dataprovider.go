package dataprovider

import (
	"database/sql"
	"fmt"

	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/shared"
)

type Endpoint struct {
	Connection *sql.DB
	Table      *model.Table
}

func (endpoint *Endpoint) GetTableSelectQuery() string {
	return fmt.Sprintf("SELECT * FROM %s.%s", endpoint.Table.Schema, endpoint.Table.GetFqName())
}

func (endpoint *Endpoint) getTableCountQuery() string {
	return fmt.Sprintf("SELECT count(*) FROM (%s) as query", endpoint.GetTableSelectQuery())
}

func (endpoint *Endpoint) GetCount() (int, error) {
	query := endpoint.getTableCountQuery()

	row := endpoint.Connection.QueryRow(query)

	var count int
	err := row.Scan(&count)

	if err != nil {
		return 0, fmt.Errorf("failed to get count: %w", err)
	}

	return count, nil
}

func (endpoint *Endpoint) FetchData() (map[string]shared.Record, error) {
	query := endpoint.GetTableSelectQuery()

	rows, err := endpoint.Connection.Query(query)

	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	defer rows.Close()

	columns, err := rows.Columns()

	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	records := make(map[string]shared.Record)

	keyName := endpoint.Table.GetKeys()[0].Name

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

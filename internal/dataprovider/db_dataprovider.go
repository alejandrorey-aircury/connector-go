package dataprovider

import (
	"database/sql"
	"fmt"

	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/shared"
)

func GetTableSelectQuery(table *model.Table) string {
	return fmt.Sprintf("SELECT * FROM %s.%s", table.Schema, table.GetFqName())
}

func FetchData(connection *sql.DB, table *model.Table) (map[string]shared.Record, error) {
	query := GetTableSelectQuery(table)

	rows, err := connection.Query(query)

	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	defer rows.Close()

	columns, err := rows.Columns()

	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	records := make(map[string]shared.Record)

	keyName := table.GetKeys()[0].Name

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

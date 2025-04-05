package dataprovider

import (
	"database/sql"
	"fmt"

	"github.com/aircury/connector/internal/shared"
)

func FetchData(db *sql.DB, query string) (map[string]shared.Record, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	records := make(map[string]shared.Record)

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

		records[fmt.Sprintf("%v", record["id"])] = record
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	return records, nil
}

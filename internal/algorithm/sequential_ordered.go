package algorithm

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/aircury/connector/internal/dataprovider"
	"github.com/aircury/connector/internal/shared"
)

func SequentialOrdered(db *sql.DB, sourceQuery string, targetQuery string) (*DiffOutput, error) {
	startTime := time.Now()

	sourceRecords, err := dataprovider.FetchData(db, sourceQuery)
	if err != nil {
		return nil, fmt.Errorf("error fetching source data: %w", err)
	}

	targetRecords, err := dataprovider.FetchData(db, targetQuery)
	if err != nil {
		return nil, fmt.Errorf("error fetching target data: %w", err)
	}

	diffOutput := &DiffOutput{
		ToInsert:    []shared.Record{},
		ToUpdate:    []shared.Record{},
		ToDelete:    []shared.Record{},
		SourceCount: len(sourceRecords),
		TargetCount: len(targetRecords),
	}

	for key, sourceRecord := range sourceRecords {
		if targetRecord, exists := targetRecords[key]; exists {
			if !recordsEqual(sourceRecord, targetRecord) {
				diffOutput.ToUpdate = append(diffOutput.ToUpdate, sourceRecord)
			}

			delete(targetRecords, key)
		} else {
			diffOutput.ToInsert = append(diffOutput.ToInsert, sourceRecord)
		}
	}

	for _, record := range targetRecords {
		diffOutput.ToDelete = append(diffOutput.ToDelete, record)
	}

	diffOutput.ProcessTime = time.Since(startTime)

	return diffOutput, nil
}

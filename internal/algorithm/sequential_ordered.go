package algorithm

import (
	"database/sql"
	"fmt"

	"github.com/aircury/connector/internal/dataprovider"
	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/shared"
)

func SequentialOrdered(sourceConnection, targetConnection *sql.DB, sourceTable, targetTable *model.Table) (*DiffOutput, error) {
	sourceRecords, err := dataprovider.FetchData(sourceConnection, sourceTable)
	if err != nil {
		return nil, fmt.Errorf("error fetching source data: %w", err)
	}

	targetRecords, err := dataprovider.FetchData(targetConnection, targetTable)
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

	return diffOutput, nil
}

package algorithm

import (
	"fmt"

	"github.com/aircury/connector/internal/endpoint"
	"github.com/aircury/connector/internal/shared"
)

func SequentialOrdered(source, target endpoint.Endpoint) (*DiffOutput, error) {
	sourceRecords, err := source.FetchData()
	if err != nil {
		return nil, fmt.Errorf("error fetching source data: %w", err)
	}

	targetRecords, err := target.FetchData()
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

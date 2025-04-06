package algorithm

import (
	"github.com/aircury/connector/internal/endpoint"
)

type SequentialOrderedAlgorithm struct {
	baseAlgorithm
}

func NewSequentialOrderedAlgorithm(source, target *endpoint.Endpoint) *SequentialOrderedAlgorithm {
	return &SequentialOrderedAlgorithm{
		baseAlgorithm: baseAlgorithm{
			Name:   "SequentialOrdered",
			Source: source,
			Target: target,
		},
	}
}

func (algorithm *SequentialOrderedAlgorithm) Run() (*DiffOutput, error) {
	sourceRecords, targetRecords, err := algorithm.FetchData()
	if err != nil {
		return nil, err
	}

	diffOutput := algorithm.CreateDiffOutput(sourceRecords, targetRecords)

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

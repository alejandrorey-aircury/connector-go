package algorithm

import (
	"github.com/aircury/connector/internal/endpoint"
)

type WriteAllAlgorithm struct {
	baseAlgorithm
}

func NewWriteAllAlgorithm(source, target *endpoint.Endpoint) *WriteAllAlgorithm {
	return &WriteAllAlgorithm{
		baseAlgorithm: baseAlgorithm{
			Name:   "WriteAll",
			Source: source,
			Target: target,
		},
	}
}

func (algorithm *WriteAllAlgorithm) Run() (*DiffOutput, error) {
	sourceRecords, targetRecords, err := algorithm.FetchData()
	if err != nil {
		return nil, err
	}

	diffOutput := algorithm.CreateDiffOutput(sourceRecords, targetRecords)

	for _, sourceRecord := range sourceRecords {
		diffOutput.ToInsert = append(diffOutput.ToInsert, sourceRecord)
	}

	for _, targetRecord := range targetRecords {
		diffOutput.ToDelete = append(diffOutput.ToDelete, targetRecord)
	}

	return diffOutput, nil
}

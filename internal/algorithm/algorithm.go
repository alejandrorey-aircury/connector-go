package algorithm

import (
	"fmt"
	"time"

	"github.com/aircury/connector/internal/endpoint"
	"github.com/aircury/connector/internal/shared"
)

type Algorithm interface {
	Run() (*DiffOutput, error)
}

type baseAlgorithm struct {
	Name   string
	Source *endpoint.Endpoint
	Target *endpoint.Endpoint
}

func (algorithm *baseAlgorithm) FetchData() (map[string]shared.Record, map[string]shared.Record, error) {
	sourceRecords, err := algorithm.Source.FetchData()
	if err != nil {
		return nil, nil, fmt.Errorf("error fetching source data: %w", err)
	}

	targetRecords, err := algorithm.Target.FetchData()
	if err != nil {
		return nil, nil, fmt.Errorf("error fetching target data: %w", err)
	}

	return sourceRecords, targetRecords, nil
}

func (algorithm *baseAlgorithm) CreateDiffOutput(sourceRecords, targetRecords map[string]shared.Record) *DiffOutput {
	return &DiffOutput{
		ToInsert:    []shared.Record{},
		ToUpdate:    []shared.Record{},
		ToDelete:    []shared.Record{},
		SourceCount: len(sourceRecords),
		TargetCount: len(targetRecords),
	}
}

type DiffOutput struct {
	ToInsert    []shared.Record
	ToUpdate    []shared.Record
	ToDelete    []shared.Record
	SourceCount int
	TargetCount int
	ProcessTime time.Duration
}

func recordsEqual(firstRecord, secondRecord shared.Record) bool {
	if len(firstRecord) != len(secondRecord) {
		return false
	}

	for key, val1 := range firstRecord {
		val2, exists := secondRecord[key]

		if !exists || !valuesEqual(val1, val2) {
			return false
		}
	}

	return true
}

func valuesEqual(val1, val2 interface{}) bool {
	if b1, ok := val1.([]byte); ok {
		if b2, ok := val2.([]byte); ok {
			return string(b1) == string(b2)
		}
		return string(b1) == fmt.Sprintf("%v", val2)
	}

	if b2, ok := val2.([]byte); ok {
		return fmt.Sprintf("%v", val1) == string(b2)
	}

	return fmt.Sprintf("%v", val1) == fmt.Sprintf("%v", val2)
}

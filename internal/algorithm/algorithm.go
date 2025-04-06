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

type dataFetchResult struct {
	records map[string]shared.Record
	err     error
}

func (algorithm *baseAlgorithm) FetchData() (map[string]shared.Record, map[string]shared.Record, error) {
	sourceChannel := make(chan dataFetchResult)
	targetChannel := make(chan dataFetchResult)

	go func() {
		records, err := algorithm.Source.FetchData()
		sourceChannel <- dataFetchResult{records: records, err: err}
	}()

	go func() {
		records, err := algorithm.Target.FetchData()
		targetChannel <- dataFetchResult{records: records, err: err}
	}()

	sourceResult := <-sourceChannel
	if sourceResult.err != nil {
		return nil, nil, fmt.Errorf("error fetching source data: %w", sourceResult.err)
	}

	targetResult := <-targetChannel
	if targetResult.err != nil {
		return nil, nil, fmt.Errorf("error fetching target data: %w", targetResult.err)
	}

	return sourceResult.records, targetResult.records, nil
}

func (algorithm *baseAlgorithm) NewDiffOutput() *DiffOutput {
	return &DiffOutput{
		ToInsert:    []shared.Record{},
		ToUpdate:    []shared.Record{},
		ToDelete:    []shared.Record{},
		SourceCount: 0,
		TargetCount: 0,
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

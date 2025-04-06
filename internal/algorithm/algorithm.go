package algorithm

import (
	"fmt"
	"time"

	"github.com/aircury/connector/internal/shared"
)

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

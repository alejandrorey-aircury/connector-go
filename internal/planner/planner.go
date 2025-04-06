package planner

import (
	"github.com/aircury/connector/internal/algorithm"
	"github.com/aircury/connector/internal/endpoint"
)

type ConnectorPlanner struct {
	Source *endpoint.Endpoint
	Target *endpoint.Endpoint
}

func (planner *ConnectorPlanner) FindBestAlgorithm() (algorithm.Algorithm, error) {
	targetCount, err := planner.Target.GetTotalCount()

	if err != nil {
		return nil, err
	}

	if targetCount == 0 {
		return algorithm.NewWriteAllAlgorithm(planner.Source, planner.Target), nil
	}

	return algorithm.NewSequentialOrderedAlgorithm(planner.Source, planner.Target), nil
}

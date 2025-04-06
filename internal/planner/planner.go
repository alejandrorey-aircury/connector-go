package planner

import (
	"github.com/aircury/connector/internal/algorithm"
	"github.com/aircury/connector/internal/endpoint"
)

type ConnectorPlanner struct {
	Source endpoint.Endpoint
	Target endpoint.Endpoint
}

func (planner *ConnectorPlanner) FindBestAlgorithm() (algorithm.Algorithm, error) {
	return algorithm.NewSequentialOrderedAlgorithm(planner.Source, planner.Target), nil
}

package connector

import (
	"fmt"

	"github.com/aircury/connector/internal/endpoint"
	"github.com/aircury/connector/internal/output"
	"github.com/aircury/connector/internal/planner"
)

type DataUpdateCommandError struct {
	Message string
}

func (e *DataUpdateCommandError) Error() string {
	return fmt.Sprintf("Error in data update command: %s", e.Message)
}

func ProcessTableDataUpdate(source, target *endpoint.Endpoint, dataUpdateTable *output.DataUpdateTable) error {
	sourceTotal, err := source.DataProvider.GetTotalCount()

	if err != nil {
		return &DataUpdateCommandError{Message: err.Error()}
	}

	targetTableName := target.Table.Name

	row, err := dataUpdateTable.GetRowByTableName(targetTableName)

	if err != nil {
		return &DataUpdateCommandError{Message: err.Error()}
	}

	row.SourceTotal = sourceTotal
	dataUpdateTable.UpdateTableRow(targetTableName, row)

	targetTotal, err := target.DataProvider.GetTotalCount()

	if err != nil {
		return &DataUpdateCommandError{Message: err.Error()}
	}

	row.TargetTotal = targetTotal
	dataUpdateTable.UpdateTableRow(targetTableName, row)

	planner := planner.ConnectorPlanner{
		Source: source,
		Target: target,
	}

	algorithm, err := planner.FindBestAlgorithm()

	if err != nil {
		return &DataUpdateCommandError{Message: err.Error()}
	}

	diff, err := algorithm.Run()

	if err != nil {
		return &DataUpdateCommandError{Message: err.Error()}
	}

	row.Inserts = len(diff.ToInsert)
	row.Updates = len(diff.ToUpdate)
	row.Drops = len(diff.ToDelete)
	dataUpdateTable.UpdateTableRow(targetTableName, row)

	return nil
}

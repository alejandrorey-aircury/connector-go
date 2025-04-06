package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/aircury/connector/internal/algorithm"
	"github.com/aircury/connector/internal/database"
	"github.com/aircury/connector/internal/dataprovider"
	definitionPkg "github.com/aircury/connector/internal/definition"
	"github.com/aircury/connector/internal/endpoint"
	"github.com/aircury/connector/internal/environment"
	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/output"
	"github.com/urfave/cli/v3"
)

type DataUpdateCommandError struct {
	Message string
}

func (e *DataUpdateCommandError) Error() string {
	return fmt.Sprintf("Error in data update command: %s", e.Message)
}

func dataUpdateCommand(_ context.Context, cli *cli.Command) error {
	startTime := time.Now()

	environment.LoadEnv()

	configurationFile := cli.String("config-file")

	definition, definitionErr := definitionPkg.ProcessDefinition(configurationFile)

	if definitionErr != nil {
		return &DataUpdateCommandError{Message: definitionErr.Error()}
	}

	sourceModel := model.ConstructModelFromDefinition(definition.Source)
	targetModel := model.ConstructModelFromDefinition(definition.Target)

	dataUpdateTable := output.NewDataUpdateTable()

	dataUpdateTable.Render()

	sourceConnection, sourceErr := database.ConnectDatabase(definition.Source.URL)
	targetConnection, targetErr := database.ConnectDatabase(definition.Target.URL)

	if sourceErr != nil {
		return &DataUpdateCommandError{Message: sourceErr.Error()}
	}

	if targetErr != nil {
		return &DataUpdateCommandError{Message: targetErr.Error()}
	}

	defer sourceConnection.Close()
	defer targetConnection.Close()

	for targetTableName, targetTable := range targetModel.Tables {
		dataUpdateTable.AddNewTableRow(targetTableName)

		row, err := dataUpdateTable.GetRowByTableName(targetTableName)

		if err != nil {
			return &DataUpdateCommandError{Message: err.Error()}
		}

		sourceTable := sourceModel.GetTableByName(targetTable.SourceTable)

		if sourceTable == nil {
			return &DataUpdateCommandError{Message: fmt.Sprintf("source table %s not found", targetTableName)}
		}

		source := endpoint.Endpoint{
			DataProvider: &dataprovider.DBDataProvider{
				Connection: sourceConnection,
				AbstractDataProvider: dataprovider.AbstractDataProvider{
					Table: sourceTable,
				},
			},
			Table: sourceTable,
		}

		target := endpoint.Endpoint{
			DataProvider: &dataprovider.DBDataProvider{
				Connection: targetConnection,
				AbstractDataProvider: dataprovider.AbstractDataProvider{
					Table: targetTable,
				},
			},
			Table: targetTable,
		}

		sourceTotal, err := source.DataProvider.GetTotalCount()

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

		diff, err := algorithm.SequentialOrdered(source, target)

		if err != nil {
			return &DataUpdateCommandError{Message: err.Error()}
		}

		row.Inserts = len(diff.ToInsert)
		row.Updates = len(diff.ToUpdate)
		row.Drops = len(diff.ToDelete)
		dataUpdateTable.UpdateTableRow(targetTableName, row)
	}

	fmt.Println("Data update process finished!!")
	fmt.Printf("Execution time: %f seconds", time.Since(startTime).Seconds())

	return nil
}

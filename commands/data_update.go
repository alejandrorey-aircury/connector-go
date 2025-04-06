package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/aircury/connector/internal/algorithm"
	"github.com/aircury/connector/internal/database"
	"github.com/aircury/connector/internal/dataprovider"
	definitionPkg "github.com/aircury/connector/internal/definition"
	"github.com/aircury/connector/internal/environment"
	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/output"
	"github.com/urfave/cli/v3"
)

func dataUpdateCmd(_ context.Context, cli *cli.Command) error {
	startTime := time.Now()

	environment.LoadEnv()

	configurationFile := cli.String("config-file")

	definition, definitionErr := definitionPkg.ProcessDefinition(configurationFile)

	if definitionErr != nil {
		return definitionErr
	}

	sourceModel := model.ConstructModelFromDefinition(definition.Source)
	targetModel := model.ConstructModelFromDefinition(definition.Target)

	dataUpdateTable := output.NewDataUpdateTable()

	dataUpdateTable.Render()

	sourceConnection, sourceErr := database.ConnectDatabase(definition.Source.URL)
	targetConnection, targetErr := database.ConnectDatabase(definition.Target.URL)

	if sourceErr != nil {
		return sourceErr
	}

	if targetErr != nil {
		return targetErr
	}

	defer sourceConnection.Close()
	defer targetConnection.Close()

	for targetTableName, targetTable := range targetModel.Tables {
		dataUpdateTable.AddNewTableRow(targetTableName)

		row, err := dataUpdateTable.GetRowByTableName(targetTableName)

		if err != nil {
			return err
		}

		sourceTable := sourceModel.GetTableByName(targetTable.SourceTable)

		if sourceTable == nil {
			return fmt.Errorf("source table %s not found", targetTableName)
		}

		source := dataprovider.Endpoint{
			Connection: sourceConnection,
			Table:      sourceTable,
		}

		target := dataprovider.Endpoint{
			Connection: targetConnection,
			Table:      targetTable,
		}

		sourceTotal, err := source.GetCount()

		if err != nil {
			return err
		}

		row.SourceTotal = sourceTotal
		dataUpdateTable.UpdateTableRow(targetTableName, row)

		targetTotal, err := target.GetCount()

		if err != nil {
			return err
		}

		row.TargetTotal = targetTotal
		dataUpdateTable.UpdateTableRow(targetTableName, row)

		diff, err := algorithm.SequentialOrdered(source, target)

		if err != nil {
			return err
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

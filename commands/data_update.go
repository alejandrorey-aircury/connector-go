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
			return fmt.Errorf("table %s not found", targetTableName)
		}

		sourceQuery := dataprovider.GetTableSelectQuery(sourceTable)
		targetQuery := dataprovider.GetTableSelectQuery(targetTable)

		var sourceTotal, targetTotal int

		sourceTotalRow := sourceConnection.QueryRow(fmt.Sprintf("SELECT count(*) FROM (%s) as query", sourceQuery))
		sourceTotalRow.Scan(&sourceTotal)

		row.SourceTotal = sourceTotal
		dataUpdateTable.UpdateTableRow(targetTableName, row)

		targetTotalRow := targetConnection.QueryRow(fmt.Sprintf("SELECT count(*) FROM (%s) as query", targetQuery))
		targetTotalRow.Scan(&targetTotal)

		row.TargetTotal = targetTotal
		dataUpdateTable.UpdateTableRow(targetTableName, row)

		diff, err := algorithm.SequentialOrdered(sourceConnection, targetConnection, sourceTable, targetTable)

		row.Inserts = len(diff.ToInsert)
		row.Updates = len(diff.ToUpdate)
		row.Drops = len(diff.ToDelete)
		dataUpdateTable.UpdateTableRow(targetTableName, row)
	}

	fmt.Println("Data update process finished!!")
	fmt.Println(fmt.Sprintf("Execution time: %f seconds", time.Since(startTime).Seconds()))

	return nil
}

package commands

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aircury/connector/internal/connector"
	"github.com/aircury/connector/internal/database"
	"github.com/aircury/connector/internal/dataprovider"
	definitionPkg "github.com/aircury/connector/internal/definition"
	"github.com/aircury/connector/internal/endpoint"
	"github.com/aircury/connector/internal/environment"
	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/output"
	"github.com/urfave/cli/v3"
)

func dataUpdateCommand(_ context.Context, cli *cli.Command) error {
	startTime := time.Now()

	dataUpdateTable := output.NewDataUpdateTable()

	dataUpdateTable.PrintCommandTitle()

	environment.LoadEnv()

	configurationFile := cli.String("config-file")

	definition, definitionErr := definitionPkg.ProcessDefinition(configurationFile)

	if definitionErr != nil {
		return &connector.DataUpdateCommandError{Message: definitionErr.Error()}
	}

	sourceModel := model.ConstructModelFromDefinition(definition.Source)
	targetModel := model.ConstructModelFromDefinition(definition.Target)

	sourceConnection, sourceErr := database.ConnectDatabase(definition.Source.URL)
	targetConnection, targetErr := database.ConnectDatabase(definition.Target.URL)

	if sourceErr != nil {
		return &connector.DataUpdateCommandError{Message: sourceErr.Error()}
	}

	if targetErr != nil {
		return &connector.DataUpdateCommandError{Message: targetErr.Error()}
	}

	defer sourceConnection.Close()
	defer targetConnection.Close()

	errorChan := make(chan error, len(targetModel.Tables))

	var waitGroup sync.WaitGroup

	for targetTableName, targetTable := range targetModel.Tables {
		waitGroup.Add(1)

		go func(tableName string, table *model.Table) {
			defer waitGroup.Done()

			dataUpdateTable.AddNewTableRow(tableName)

			sourceTable := sourceModel.GetTableByName(table.SourceTable)

			if sourceTable == nil {
				errorChan <- &connector.DataUpdateCommandError{Message: fmt.Sprintf("source table %s not found", tableName)}
				return
			}

			source := endpoint.Endpoint{
				DataProvider: dataprovider.NewDBDataProvider(sourceConnection, sourceTable),
				Table:        sourceTable,
			}

			target := endpoint.Endpoint{
				DataProvider: dataprovider.NewDBDataProvider(targetConnection, table),
				Table:        table,
			}

			err := connector.ProcessTableDataUpdate(&source, &target, dataUpdateTable)

			if err != nil {
				errorChan <- err
			}
		}(targetTableName, targetTable)
	}

	waitGroup.Wait()
	close(errorChan)

	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	successMessage := fmt.Sprintf("Data update process finished!! Execution time: %f seconds", time.Since(startTime).Seconds())

	dataUpdateTable.Success(successMessage)

	return nil
}

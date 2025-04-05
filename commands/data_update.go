package commands

import (
	"context"
	"fmt"

	"github.com/aircury/connector/internal/algorithm"
	"github.com/aircury/connector/internal/database"
	definitionPkg "github.com/aircury/connector/internal/definition"
	"github.com/aircury/connector/internal/environment"
	"github.com/aircury/connector/internal/output"
	"github.com/urfave/cli/v3"
)

func dataUpdateCmd(_ context.Context, cli *cli.Command) error {
	environment.LoadEnv()

	configurationFile := cli.String("config-file")

	definition, err := definitionPkg.ProcessDefinition(configurationFile)

	if err != nil {
		return err
	}

	dataUpdateTable := output.NewDataUpdateTable()

	dataUpdateTable.Render()

	db, err := database.ConnectDatabase(definition.Source.URL)

	defer db.Close()

	if err != nil {
		return err
	}

	sourceQuery := "SELECT * FROM demos.caching_source"
	targetQuery := "SELECT * FROM demos.caching_target"

	tableName := "caching_target"

	dataUpdateTable.AddNewTableRow(tableName)

	row, err := dataUpdateTable.GetRowByTableName(tableName)

	if err != nil {
		return err
	}

	var sourceTotal, targetTotal int

	sourceTotalRow := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM (%s) as query", sourceQuery))
	sourceTotalRow.Scan(&sourceTotal)

	row.SourceTotal = sourceTotal
	dataUpdateTable.UpdateTableRow(tableName, row)

	targetTotalRow := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM (%s) as query", targetQuery))
	targetTotalRow.Scan(&targetTotal)

	row.TargetTotal = targetTotal
	dataUpdateTable.UpdateTableRow(tableName, row)

	diff, err := algorithm.SequentialOrdered(db, sourceQuery, targetQuery)

	row.Inserts = len(diff.ToInsert)
	row.Updates = len(diff.ToUpdate)
	row.Drops = len(diff.ToDelete)
	dataUpdateTable.UpdateTableRow(tableName, row)

	if err != nil {
		return err
	}

	fmt.Println(fmt.Sprintf("Process time: %f seconds", diff.ProcessTime.Seconds()))

	fmt.Println("Data update process finished!!")

	return nil
}

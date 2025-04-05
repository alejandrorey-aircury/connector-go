package commands

import (
	"context"
	"fmt"

	"github.com/aircury/connector/internal/algorithm"
	"github.com/aircury/connector/internal/database"
	definitionPkg "github.com/aircury/connector/internal/definition"
	"github.com/aircury/connector/internal/environment"
	"github.com/urfave/cli/v3"
)

func dataUpdateCmd(_ context.Context, cli *cli.Command) error {
	environment.LoadEnv()

	configurationFile := cli.String("config-file")

	definition, err := definitionPkg.ProcessDefinition(configurationFile)

	if err != nil {
		return err
	}

	db, err := database.ConnectDatabase(definition.Source.URL)

	defer db.Close()

	if err != nil {
		return err
	}

	sourceQuery := "SELECT * FROM demos.caching_source;"
	targetQuery := "SELECT * FROM demos.caching_target;"

	diff, err := algorithm.SequentialOrdered(db, sourceQuery, targetQuery)

	if err != nil {
		return err
	}

	fmt.Println(fmt.Sprintf("Extracted %d records from source", diff.SourceCount))
	fmt.Println(fmt.Sprintf("Extracted %d records from source", diff.TargetCount))

	fmt.Println(fmt.Sprintf("Records to insert: %d", len(diff.ToInsert)))
	fmt.Println(fmt.Sprintf("Records to update: %d", len(diff.ToUpdate)))
	fmt.Println(fmt.Sprintf("Records to delete: %d", len(diff.ToDelete)))

	fmt.Println(fmt.Sprintf("Process time: %f seconds", diff.ProcessTime.Seconds()))

	fmt.Println("Data update process finished!!")

	return nil
}

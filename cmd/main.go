package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aircury/connector/internal/algorithm"
	"github.com/aircury/connector/internal/command"
	"github.com/aircury/connector/internal/database"
	definitionPkg "github.com/aircury/connector/internal/definition"
	"github.com/aircury/connector/internal/environment"
	_ "github.com/lib/pq"
)

func main() {
	environment.LoadEnv()

	commandParameters := command.Parameters{}

	command.ConfigureParameters(&commandParameters)

	command.LoadParameters()

	if commandParameters.ConfigurationFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	definition, err := definitionPkg.ProcessDefinition(commandParameters.ConfigurationFile)

	if err != nil {
		log.Fatal(err)
	}

	db, err := database.ConnectDatabase(definition.Source.URL)

	defer db.Close()

	if err != nil {
		log.Fatal(err)
	}

	sourceQuery := "SELECT * FROM demos.caching_source;"
	targetQuery := "SELECT * FROM demos.caching_target;"

	diff, err := algorithm.SequentialOrdered(db, sourceQuery, targetQuery)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(fmt.Sprintf("Extracted %d records from source", diff.SourceCount))
	fmt.Println(fmt.Sprintf("Extracted %d records from source", diff.TargetCount))

	fmt.Println(fmt.Sprintf("Records to insert: %d", len(diff.ToInsert)))
	fmt.Println(fmt.Sprintf("Records to update: %d", len(diff.ToUpdate)))
	fmt.Println(fmt.Sprintf("Records to delete: %d", len(diff.ToDelete)))

	fmt.Println(fmt.Sprintf("Process time: %f seconds", diff.ProcessTime.Seconds()))
}

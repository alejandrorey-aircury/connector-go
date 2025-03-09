package main

import (
	"flag"
	"log"
	"os"

	"github.com/aircury/connector/internal/command"
	definitionPkg "github.com/aircury/connector/internal/definition"
	"github.com/aircury/connector/internal/environment"
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

	definitionPkg.PrintDefinition(definition)
}

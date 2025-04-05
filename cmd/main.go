package main

import (
	"context"
	"log"
	"os"

	"github.com/aircury/connector/commands"
	"github.com/urfave/cli/v3"

	_ "github.com/lib/pq"
)

const AppName = "connector"
const AppDescription = "Connector project written in Go language"

func main() {
	cmd := &cli.Command{
		Name:     AppName,
		Usage:    AppDescription,
		Commands: commands.Commands,
	}

	err := cmd.Run(context.Background(), os.Args)

	if err != nil {
		log.Fatal(err)
	}
}

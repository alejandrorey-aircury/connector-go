package commands

import (
	"github.com/urfave/cli/v3"
)

var Commands = []*cli.Command{
	{
		Name:   "data:update",
		Usage:  "This commands replicates source into target",
		Action: dataUpdateCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config-file",
				Aliases: []string{"c"},
				Usage:   "Path to the configuration file",
			},
		},
	},
}

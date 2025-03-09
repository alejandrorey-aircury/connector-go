package command

import "flag"

func ConfigureParameters(commandParameters *Parameters) {
	flag.StringVar(&commandParameters.ConfigurationFile, "file", "", "Path to the YAML configuration file")
}

func LoadParameters() {
	flag.Parse()
}

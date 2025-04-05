package definition

import (
	"os"

	"gopkg.in/yaml.v3"
)

func parseDefinition(configurationFilePath string) (Definition, error) {
	configurationFile, err := os.ReadFile(configurationFilePath)

	if err != nil {
		return Definition{}, err
	}

	var definition Definition

	err = yaml.Unmarshal(configurationFile, &definition)

	return definition, err
}

func ProcessDefinition(configurationFilePath string) (Definition, error) {
	definition, err := parseDefinition(configurationFilePath)

	if err != nil {
		return Definition{}, &ProcessError{Message: err.Error()}
	}

	definition.Source.URL = os.ExpandEnv(definition.Source.URL)
	definition.Target.URL = os.ExpandEnv(definition.Target.URL)

	return definition, nil
}

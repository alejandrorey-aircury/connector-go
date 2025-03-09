package definition

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

func PrintDefinition(definition Definition) {
	jsonDataBytes, _ := json.MarshalIndent(definition, "", "  ")

	jsonData := string(jsonDataBytes)

	fmt.Println(jsonData)
}

func ProcessDefinition(configurationFilePath string) (Definition, error) {
	configurationFile, err := os.ReadFile(configurationFilePath)

	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}

	var definition Definition

	err = yaml.Unmarshal(configurationFile, &definition)

	if err != nil {
		log.Fatalf("Error parsing YAML: %v", err)
	}

	definition.Source.URL = os.ExpandEnv(definition.Source.URL)
	definition.Target.URL = os.ExpandEnv(definition.Target.URL)

	return definition, nil
}

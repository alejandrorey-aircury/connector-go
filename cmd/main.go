package main

import (
	"flag"
	"github.com/joho/godotenv"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Definition struct {
	Source EndpointDefinition[SourceTableDefinition] `yaml:"source"`
	Target EndpointDefinition[TargetTableDefinition] `yaml:"target"`
}

type EndpointDefinition[TableDefinition SourceTableDefinition | TargetTableDefinition] struct {
	URL   string                           `yaml:"url"`
	Model ModelDefinition[TableDefinition] `yaml:"model"`
}

type ModelDefinition[TableDefinition SourceTableDefinition | TargetTableDefinition] struct {
	Tables map[string]TableDefinition `yaml:"tables"`
}

type TableDefinition struct {
	Schema  string            `yaml:"schema,omitempty"`
	Columns map[string]string `yaml:"columns,omitempty"`
	Keys    []string          `yaml:"keys,omitempty"`
}

type SourceTableDefinition struct {
	TableDefinition `yaml:",inline"`
}

type TargetTableDefinition struct {
	TableDefinition `yaml:",inline"`
	Inherit         string `yaml:"inherit,omitempty"`
	SourceTable     string `yaml:"sourceTable,omitempty"`
}

func main() {
	err := godotenv.Load()

	if err != nil {
		log.Fatal("Error loading .env file")
	}

	var configurationFile string

	flag.StringVar(&configurationFile, "file", "", "Path to the YAML configuration file")
	flag.Parse()

	if configurationFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	definitionData, err := os.ReadFile(configurationFile)

	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}

	var definition Definition
	err = yaml.Unmarshal(definitionData, &definition)

	if err != nil {
		log.Fatalf("Error parsing YAML: %v", err)
	}

	definition.Source.URL = os.ExpandEnv(definition.Source.URL)
	definition.Target.URL = os.ExpandEnv(definition.Target.URL)

	log.Println(definition)
}

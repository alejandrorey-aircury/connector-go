package definition

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

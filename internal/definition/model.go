package definition

type Definition struct {
	Source EndpointDefinition `yaml:"source"`
	Target EndpointDefinition `yaml:"target"`
}

type EndpointDefinition struct {
	URL   string          `yaml:"url"`
	Model ModelDefinition `yaml:"model"`
}

type ModelDefinition struct {
	Tables map[string]TableDefinition `yaml:"tables"`
}

type TableDefinition struct {
	Schema        string                 `yaml:"schema,omitempty"`
	ResourceName  string                 `yaml:"resourceName,omitempty"`
	Columns       map[string]interface{} `yaml:"columns,omitempty"`
	Keys          []string               `yaml:"keys,omitempty"`
	Indices       []string               `yaml:"indices,omitempty"`
	UniqueIndices []string               `yaml:"uniqueIndices,omitempty"`
	SourceTable   string                 `yaml:"sourceTable,omitempty"`
}

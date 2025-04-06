package model

type Table struct {
	Schema       string
	Name         string
	ResourceName string
	Columns      map[string]*Column
	SourceTable  string
}

func (table *Table) GetKeys() []*Column {
	var keys []*Column

	for _, column := range table.Columns {
		if column.isKey {
			keys = append(keys, column)
		}
	}

	return keys
}

func (table *Table) GetFqName() string {
	if len(table.ResourceName) == 0 {
		return table.Name
	}

	return table.ResourceName
}

func (table *Table) GetColumnNames() []string {
	columnNames := make([]string, 0, len(table.Columns))

	for columnName := range table.Columns {
		columnNames = append(columnNames, columnName)
	}

	return columnNames
}

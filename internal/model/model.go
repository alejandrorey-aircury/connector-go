package model

import (
	"fmt"

	"github.com/aircury/connector/internal/definition"
)

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

type Column struct {
	Type     string
	Name     string
	Nullable bool
	isKey    bool
}

type Model struct {
	Tables map[string]*Table
}

func (model *Model) GetTableByName(name string) *Table {
	return model.Tables[name]
}

func ConstructModelFromDefinition(definition definition.EndpointDefinition) *Model {
	model := &Model{
		Tables: make(map[string]*Table),
	}

	for tableName, tableDefinition := range definition.Model.Tables {
		table := &Table{
			Schema:       tableDefinition.Schema,
			Name:         tableName,
			ResourceName: tableDefinition.ResourceName,
			SourceTable:  tableDefinition.SourceTable,
			Columns:      make(map[string]*Column),
		}

		for columnName, columnDefinition := range tableDefinition.Columns {
			column, _ := processColumnDefinition(columnName, columnDefinition)

			for _, key := range tableDefinition.Keys {
				if key == columnName {
					column.isKey = true
					break
				}
			}

			table.Columns[columnName] = column
		}

		model.Tables[tableName] = table
	}

	return model
}

func processColumnDefinition(columnName string, columnDefinition interface{}) (*Column, error) {
	column := &Column{
		Name:     columnName,
		Nullable: true,
		isKey:    false,
	}

	switch columnDef := columnDefinition.(type) {
	case string:
		column.Type = columnDef

	case map[string]interface{}:
		if columnType, ok := columnDef["type"]; ok {
			if columnTypeString, ok := columnType.(string); ok {
				column.Type = columnTypeString
			}
		}

		if isNullable, ok := columnDef["nullable"]; ok {
			if isNullableBool, ok := isNullable.(bool); ok {
				column.Nullable = isNullableBool
			}
		}

	default:
		return nil, fmt.Errorf("invalid column definition for column: %s", columnName)
	}

	return column, nil
}

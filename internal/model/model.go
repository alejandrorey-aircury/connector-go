package model

import (
	"fmt"

	"github.com/aircury/connector/internal/definition"
)

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

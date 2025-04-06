package dataprovider

import (
	"fmt"
	"strings"

	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/shared"
)

type AbstractDataProvider struct {
	Table *model.Table
}

func (abstractDataProvider *AbstractDataProvider) FilterRecordByModelColumns(record shared.Record) shared.Record {
	filteredRecord := make(shared.Record)

	for columnName := range abstractDataProvider.Table.Columns {
		if value, exists := record[columnName]; exists {
			filteredRecord[columnName] = value
		}
	}

	return filteredRecord
}

func (abstractDataProvider *AbstractDataProvider) GetRecordIdentifier(record shared.Record) string {
	keys := abstractDataProvider.Table.GetKeys()

	if len(keys) == 1 {
		return fmt.Sprintf("%v", record[keys[0].Name])
	}

	keyValues := make([]string, len(keys))
	for i, key := range keys {
		keyValues[i] = fmt.Sprintf("%v", record[key.Name])
	}

	return strings.Join(keyValues, "-")
}

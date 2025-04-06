package dataprovider

import (
	"fmt"
	"strings"

	"github.com/aircury/connector/internal/model"
	"github.com/aircury/connector/internal/shared"
)

type DataProvider interface {
	GetTotalCount() (int, error)
	FetchData() (map[string]shared.Record, error)
}

type baseDataProvider struct {
	Table *model.Table
}

func newBaseDataProvider(table *model.Table) baseDataProvider {
	return baseDataProvider{
		Table: table,
	}
}

func (dataProvider *baseDataProvider) FilterRecordByModelColumns(record shared.Record) shared.Record {
	filteredRecord := make(shared.Record)

	for columnName := range dataProvider.Table.Columns {
		if value, exists := record[columnName]; exists {
			filteredRecord[columnName] = value
		}
	}

	return filteredRecord
}

func (dataProvider *baseDataProvider) GetRecordIdentifier(record shared.Record) string {
	keys := dataProvider.Table.GetKeys()

	if len(keys) == 1 {
		return fmt.Sprintf("%v", record[keys[0].Name])
	}

	keyValues := make([]string, len(keys))
	for i, key := range keys {
		keyValues[i] = fmt.Sprintf("%v", record[key.Name])
	}

	return strings.Join(keyValues, "-")
}

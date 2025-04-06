package dataprovider

import (
	"github.com/aircury/connector/internal/shared"
)

type ArrayDataProvider struct {
	AbstractDataProvider
	Data []shared.Record
}

func (dataProvider *ArrayDataProvider) GetTotalCount() (int, error) {
	return len(dataProvider.Data), nil
}

func (dataProvider *ArrayDataProvider) FetchData() (map[string]shared.Record, error) {
	records := make(map[string]shared.Record)

	for _, record := range dataProvider.Data {
		filteredRecord := dataProvider.FilterRecordByModelColumns(record)
		keyValue := dataProvider.GetRecordIdentifier(filteredRecord)

		records[keyValue] = filteredRecord
	}

	return records, nil
}

package endpoint

import (
	"github.com/aircury/connector/internal/dataprovider"
	"github.com/aircury/connector/internal/model"
)

type Endpoint struct {
	dataprovider.DataProvider
	Table *model.Table
}

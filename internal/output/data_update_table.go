package output

import (
	"fmt"
	"os"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pterm/pterm"
)

type DataUpdateTableRow struct {
	TableName   string
	SourceTotal int
	TargetTotal int
	Inserts     int
	Updates     int
	Drops       int
	ProcessTime float64
	StartTime   time.Time
}

func (row DataUpdateTableRow) GetRowValues() table.Row {
	return table.Row{
		row.TableName,
		row.SourceTotal,
		row.TargetTotal,
		row.Inserts,
		row.Updates,
		row.Drops,
		row.ProcessTime,
	}
}

func NewDataUpdateTable() *DataUpdateTable {
	writer := table.NewWriter()

	writer.SetOutputMirror(os.Stdout)
	writer.SetStyle(table.StyleLight)

	dataUpdateTable := &DataUpdateTable{
		writer,
		map[string]DataUpdateTableRow{},
	}

	dataUpdateTable.SetHeaders(table.Row{"Target Table", "Source", "Target", "Inserts", "Updates", "Drops", "Process Time (s)"})

	return dataUpdateTable
}

type DataUpdateTable struct {
	Writer table.Writer
	Rows   map[string]DataUpdateTableRow
}

func (dataUpdateTable *DataUpdateTable) Render() {
	dataUpdateTable.Writer.Render()
}

func (dataUpdateTable *DataUpdateTable) SetHeaders(headers table.Row) {
	dataUpdateTable.Writer.AppendHeader(headers)
}

func (dataUpdateTable *DataUpdateTable) AddNewTableRow(tableName string) {
	row := DataUpdateTableRow{
		TableName:   tableName,
		SourceTotal: 0,
		TargetTotal: 0,
		Inserts:     0,
		Updates:     0,
		Drops:       0,
		ProcessTime: 0,
		StartTime:   time.Now(),
	}

	dataUpdateTable.Rows[tableName] = row

	dataUpdateTable.RefreshTable()
}

func (dataUpdateTable *DataUpdateTable) UpdateTableRow(tableName string, row DataUpdateTableRow) {
	row.ProcessTime = time.Since(row.StartTime).Seconds()

	dataUpdateTable.Rows[tableName] = row

	dataUpdateTable.RefreshTable()
}

func (dataUpdateTable *DataUpdateTable) RefreshTable() {
	dataUpdateTable.Writer.ResetRows()

	for _, row := range dataUpdateTable.Rows {
		dataUpdateTable.Writer.AppendRow(row.GetRowValues())
	}

	dataUpdateTable.Render()
}

func (dataUpdateTable *DataUpdateTable) GetRowByTableName(tableName string) (DataUpdateTableRow, error) {
	for _, row := range dataUpdateTable.Rows {
		if row.TableName == tableName {
			return row, nil
		}
	}

	return DataUpdateTableRow{}, fmt.Errorf("no row with table name %s found", tableName)
}

func (dataUpdateTable *DataUpdateTable) PrintCommandTitle() {
	pterm.Println()
	pterm.DefaultHeader.Println("Connector Data Update Command")
	pterm.Println()
}

func (dataUpdateTable *DataUpdateTable) Success(message string) {
	headerPrinter := pterm.HeaderPrinter{
		TextStyle:       pterm.NewStyle(pterm.FgBlack),
		BackgroundStyle: pterm.NewStyle(pterm.BgGreen),
		Margin:          10,
		FullWidth:       false,
	}

	pterm.Println()
	headerPrinter.Println(message)
	pterm.Println()
}

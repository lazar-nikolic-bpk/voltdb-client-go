package table

import (
	"database/sql/driver"
	"voltdb-client-go/voltdbclient/common"
)

type VoltTable struct {
	Columns []Column
	Rows    [][]driver.Value
}

type Column struct {
	Type int8
	Name string
}

func (vt *VoltTable) RowNum() int {
	return len(vt.Rows)
}

func (vt *VoltTable) SetColumns(c []Column) (*VoltTable, error) {
	vt.Columns = c
	return vt, nil
}

func (vt *VoltTable) AddRow(rows []driver.Value) *VoltTable {
	vt.Rows = append(vt.Rows, rows)
	return vt
}

// +-------------------------------------------------------------------------------------------------------------------+------------+
// | Total table length | Table Metadata Length | Status Code | Column Count | Column Types | Column Names | Row Count | Row Length |
// +-------------------------------------------------------------------------------------------------------------------+------------+
// | 4 Bytes			 | 4 Bytes				 | 1 Bytes	   | 2 Bytes	  | variable	 | variable 	| 4	Bytes  | rowNum * 4 |
// +-------------------------------------------------------------------------------------------------------------------+------------+
func (vt *VoltTable) MetaLen() int {
	// Column types and names combined
	colNamesLen := 0
	for _, col := range vt.Columns {
		colNamesLen += 1 + 4 + len(col.Name)
	}
	return 1 + 2 + colNamesLen
}

func (vt *VoltTable) Len() int {
	rowLength := 0
	for _, row := range vt.Rows {
		rowLength += 4
		for _, v := range row {
			rowLength += common.TypeLen(v)
		}
	}
	// meta header + meta length value
	// total table length not included
	return 4 + 4 + vt.MetaLen() + rowLength
}

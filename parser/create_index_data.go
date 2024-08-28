package parser

import "fmt"

type IndexData struct {
	idx_name string
	tbl_name string
	fld_name string
}

func NewIndexData(idx_name string, tbl_name string, fld_name string) *IndexData {
	return &IndexData{
		idx_name: idx_name,
		tbl_name: tbl_name,
		fld_name: fld_name,
	}
}

func (id *IndexData) IndexName() string {
	return id.idx_name
}

func (id *IndexData) TableName() string {
	return id.tbl_name
}

func (id *IndexData) FieldName() string {
	return id.fld_name
}

func (id *IndexData) ToString() string {
	str := fmt.Sprintf("index_name: %s, table_name: %s, fld_name: %s\n", id.idx_name, id.tbl_name, id.fld_name)
	return str
}

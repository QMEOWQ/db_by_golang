package parser

import (
	e_rm "entry_record_manager"
	"fmt"
)

type CreateTableData struct {
	tbl_name string
	sch      *e_rm.Schema
}

func NewCreateTabLeData(tbl_name string, sch *e_rm.Schema) *CreateTableData {
	return &CreateTableData{
		tbl_name: tbl_name,
		sch:      sch,
	}
}

func (ctd *CreateTableData) TableName() string {
	return ctd.tbl_name
}

func (ctd *CreateTableData) NewSchema() *e_rm.Schema {
	return ctd.sch
}

func (ctd *CreateTableData) ToString() string {
	str := fmt.Sprintf("table_name: %s, schema: %v", ctd.tbl_name, ctd.sch)
	return str
}

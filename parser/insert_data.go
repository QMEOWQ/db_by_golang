package parser

import (
	"fmt"
	"query"
	"strings"
)

type InsertData struct {
	tbl_name string
	flds     []string
	vals     []*query.Constant
}

func NewInsertData(tbl_name string, flds []string, vals []*query.Constant) *InsertData {
	return &InsertData{
		tbl_name: tbl_name,
		flds:     flds,
		vals:     vals,
	}
}

func (id *InsertData) TableName() string {
	return id.tbl_name
}

func (id *InsertData) Fields() []string {
	return id.flds
}

func (id *InsertData) Vals() []*query.Constant {
	return id.vals
}

func (id *InsertData) ToString() string {
	fldsStr := strings.Join(id.flds, ", ")
	valsStr := ""
	for _, val := range id.vals {
		valsStr += val.ToString() + ", "
	}

	str := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", id.tbl_name, fldsStr, valsStr)
	return str
}

package parser

import (
	"fmt"
	"query"
)

type ModifyData struct {
	tbl_name string
	fld_name string
	new_val  *query.Expression
	pred     *query.Predicate
}

func NewModifyData(tbl_name string, fld_name string, new_val *query.Expression, pred *query.Predicate) *ModifyData {
	return &ModifyData{
		tbl_name: tbl_name,
		fld_name: fld_name,
		new_val:  new_val,
		pred:     pred,
	}
}

func (md *ModifyData) TableName() string {
	return md.tbl_name
}

func (md *ModifyData) TargetField() string {
	return md.fld_name
}

func (md *ModifyData) NewVal() *query.Expression {
	return md.new_val
}

func (md *ModifyData) Pred() *query.Predicate {
	return md.pred
}

func (md *ModifyData) ToString() string {
	str := fmt.Sprintf("ModifyData: tbl_name: %s, fld_name: %s, new_val: %s, pred: %s", md.tbl_name, md.fld_name, md.new_val.ToString(), md.pred.ToString())
	return str
}

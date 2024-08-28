package parser

import (
	"fmt"
	"query"
)

type DeleteData struct {
	tbl_name string
	pred     *query.Predicate
}

func NewDeleteData(tbl_name string, pred *query.Predicate) *DeleteData {
	return &DeleteData{
		tbl_name: tbl_name,
		pred:     pred,
	}
}

func (dd *DeleteData) TableName() string {
	return dd.tbl_name
}

func (dd *DeleteData) Pred() *query.Predicate {
	return dd.pred
}

func (dd *DeleteData) ToString() string {
	str := fmt.Sprintf("DELETE FROM %s WHERE %s", dd.tbl_name, dd.pred.ToString())
	return str
}

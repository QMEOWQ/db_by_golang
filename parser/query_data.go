package parser

import (
	"query"
)

type QueryData struct {
	fields []string
	tables []string
	pred   *query.Predicate
}

func NewQueryData(fields []string, tables []string, pred *query.Predicate) *QueryData {
	return &QueryData{
		fields: fields,
		tables: tables,
		pred:   pred,
	}
}

func (qd *QueryData) Fields() []string {
	return qd.fields
}

func (qd *QueryData) Tables() []string {
	return qd.tables
}

func (qd *QueryData) Predicate() *query.Predicate {
	return qd.pred
}

func (qd *QueryData) ToString() string {
	res := " select "
	for _, fld_name := range qd.fields {
		res += fld_name + ","
	}

	//去掉最后一个逗号
	res = res[0 : len(res)-1]
	res += " from "
	for _, tbl_name := range qd.tables {
		res += tbl_name + ","
	}

	//去掉最后一个逗号
	res = res[0 : len(res)-1]
	pred_str := qd.pred.ToString()
	if pred_str != "" {
		res += " where " + pred_str
	}

	return res
}

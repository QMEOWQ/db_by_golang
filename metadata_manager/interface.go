package metadata_manager

import (
	ts "transaction"
	e_rm "entry_record_manager"
	"query"
)

type TableManagerInterface interface {
	CreateTable(tbl_name string, sch *e_rm.Schema, ts *ts.Transaction)
	GetLayout(tbl_name string, ts *ts.Transaction) *e_rm.Layout 
}

type Index interface {
	FirstQualify(search_key *query.Constant) //指向第一条满足查询条件的记录
	Next() bool //是否还有满足条件的记录
	GetDataRID() *e_rm.RID
	Insert(data_val *query.Constant, data_rid *e_rm.RID)
	Delete(data_val *query.Constant, data_rid *e_rm.RID)
	Close()
}


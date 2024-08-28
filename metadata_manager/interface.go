package metadata_manager

import (
	ts "transaction"
	e_rm "entry_record_manager"
)

type TableManagerInterface interface {
	CreateTable(tbl_name string, sch *e_rm.Schema, ts *ts.Transaction)
	GetLayout(tbl_name string, ts *ts.Transaction) *e_rm.Layout 
}


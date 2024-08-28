package planner

import (
	e_rm "entry_record_manager"
	mdm "metadata_manager"
	"query"
	ts "transaction"
)

type TablePlan struct {
	ts       *ts.Transaction
	tbl_name string
	layout   *e_rm.Layout
	si       *mdm.StatInfo
}

func NewTablePlan(ts *ts.Transaction, tbl_name string, mdm *mdm.MetaDataManager) *TablePlan {
	table_planner := TablePlan{
		ts:       ts,
		tbl_name: tbl_name,
	}

	table_planner.layout = mdm.GetLayout(table_planner.tbl_name, table_planner.ts)
	table_planner.si = mdm.GetStatInfo(table_planner.tbl_name, table_planner.layout, table_planner.ts)

	return &table_planner
}

func (tp *TablePlan) Open() interface{} {
	return query.NewTableScan(tp.ts, tp.tbl_name, tp.layout)
}

func (tp *TablePlan) BlocksAccessed() int {
	return tp.si.BlockAccessed()
}

func (tp *TablePlan) RecordsOutput() int {
	return tp.si.RecordsOutput()
}

func (tp *TablePlan) DistinctVals(tbl_name string) int {
	return tp.si.DistinctVals(tbl_name)
}

func (tp *TablePlan) Schema() e_rm.SchemaInterface {
	return tp.layout.Schema()
}

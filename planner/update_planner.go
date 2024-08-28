package planner

import (
	"metadata_manager"
	"parser"
	"query"
	ts "transaction"
)

type BasicUpdatePlanner struct {
	mdm *metadata_manager.MetaDataManager
}

func NewBasicUpdatePlanner(mdm *metadata_manager.MetaDataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{
		mdm: mdm,
	}
}

func (bup *BasicUpdatePlanner) ExecuteCreateTable(data *parser.CreateTableData, ts *ts.Transaction) int {
	bup.mdm.CreateTable(data.TableName(), data.NewSchema(), ts)
	return 0
}

func (bup *BasicUpdatePlanner) ExecuteCreateView(data *parser.ViewData, ts *ts.Transaction) int {
	bup.mdm.CreateView(data.ViewName(), data.ViewDef(), ts)
	return 0
}

func (bup *BasicUpdatePlanner) ExecuteCreateIndex(data *parser.IndexData, ts *ts.Transaction) int {
	// TODO: Implement index creation
	bup.mdm.CreateIndex(data.IndexName(), data.TableName(), data.FieldName(), ts)

	return 0
}

func (bup *BasicUpdatePlanner) ExecuteInsert(data *parser.InsertData, ts *ts.Transaction) int {
	table_plan := NewTablePlan(ts, data.TableName(), bup.mdm)
	update_scan := table_plan.Open().(*query.TableScan)

	update_scan.Insert()
	insert_flds := data.Fields()
	insert_vals := data.Vals()

	for i := 0; i < len(insert_flds); i++ {
		update_scan.SetVal(insert_flds[i], insert_vals[i])
	}

	update_scan.Close()

	return 1
}

func (bup *BasicUpdatePlanner) ExecuteModify(data *parser.ModifyData, ts *ts.Transaction) int {
	table_plan := NewTablePlan(ts, data.TableName(), bup.mdm)
	select_plan := NewSelectPlan(table_plan, data.Pred())
	scan := select_plan.Open()
	update_scan := scan.(*query.SelectScan)
	cnt := 0

	for update_scan.Next() {
		val := data.NewVal().Evaluate(scan.(query.Scan))
		update_scan.SetVal(data.TargetField(), val)
		cnt++
	}

	return cnt
}

func (bup *BasicUpdatePlanner) ExecuteDelete(data *parser.DeleteData, ts *ts.Transaction) int {
	table_plan := NewTablePlan(ts, data.TableName(), bup.mdm)
	select_plan := NewSelectPlan(table_plan, data.Pred())
	scan := select_plan.Open()
	update_scan := scan.(*query.SelectScan)
	cnt := 0

	for update_scan.Next() {
		update_scan.Delete()
		cnt++
	}

	update_scan.Close()

	return cnt
}



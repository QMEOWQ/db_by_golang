package planner

import (
	e_rm "entry_record_manager"
	"parser"
	ts "transaction"
)

type Plan interface {
	Open() interface{}
	BlocksAccessed() int              //对应 B(s) : 记录占据的块数
	RecordsOutput() int               //对应 R(s) : 记录的数量
	DistinctVals(fld_name string) int // 对应V(s, F) : 字段 fld_name 的不同值数量
	Schema() e_rm.SchemaInterface
}

type QueryPlanner interface {
	CreatePlan(data *parser.QueryData, ts *ts.Transaction) Plan
}

type UpdatePlanner interface {
	//解释执行相应语句，并返回执行结果。eg.相应的记录条数

	ExecuteCreateTable(data *parser.CreateTableData, ts *ts.Transaction) int
	ExecuteCreateView(data *parser.ViewData, ts *ts.Transaction) int
	ExecuteCreateIndex(data *parser.IndexData, ts *ts.Transaction) int

	ExecuteInsert(data *parser.InsertData, ts *ts.Transaction) int
	ExecuteDelete(data *parser.DeleteData, ts *ts.Transaction) int
	ExecuteModify(data *parser.ModifyData, ts *ts.Transaction) int
}

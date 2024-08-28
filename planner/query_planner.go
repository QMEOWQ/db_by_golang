package planner

import (
	"metadata_manager"
	"parser"
	ts "transaction"
)

type BasicQueryPlanner struct {
	mdm *metadata_manager.MetaDataManager
}

func NewBasicQueryPlanner(mdm *metadata_manager.MetaDataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{
		mdm: mdm,
	}
}

func (bqp *BasicQueryPlanner) CreatePlan(data *parser.QueryData, ts *ts.Transaction) Plan {
	//创建query_data对象中的表
	plans := make([]Plan, 0)
	tables := data.Tables()

	for _, tbl_name := range tables {
		//获取对应视图的sql代码
		view_def := bqp.mdm.GetViewDef(tbl_name, ts)
		if view_def != "" {
			//创建表对应的视图
			parser := parser.NewSqlParser(view_def)
			view_data := parser.Query()
			//创建视图对应的查询规划器
			plans = append(plans, bqp.CreatePlan(view_data, ts))
		} else {
			plans = append(plans, NewTablePlan(ts, tbl_name, bqp.mdm))
		}
	}

	//给定表依次执行 Product 操作, product 即将表两两合并
	plan := plans[0]
	other_plans := plans[1:]

	for _, next_plan := range other_plans {
		plan = NewProductPlan(plan, next_plan)
	}

	//project 即将查询结果中只保留所需字段(选出相应列)
	return NewProjectPlan(plan, data.Fields())
}

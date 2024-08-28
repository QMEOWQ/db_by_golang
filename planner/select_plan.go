package planner

import (
	e_rm "entry_record_manager"
	"query"
)

type SelectPlan struct {
	p Plan
	pred *query.Predicate
}

func NewSelectPlan(p Plan, pred *query.Predicate) *SelectPlan {
	return &SelectPlan{
		p: p,
		pred: pred,
	}
}

func (sp *SelectPlan) Open() interface{} {
	scan := sp.p.Open()
	updateScan, ok := scan.(query.UpdateScan)
	if !ok {
		updateScanWrapper := query.NewUpdateScanWrapper(scan.(query.Scan))
		return query.NewSelectScan(updateScanWrapper, sp.pred)
	}
	return query.NewSelectScan(updateScan, sp.pred)
}	

func (sp *SelectPlan) BlocksAccessed() int {
	return sp.p.BlocksAccessed()
}

func (sp *SelectPlan) RecordsOutput() int {
	return sp.p.RecordsOutput() / sp.pred.ReductionFactor(sp.p)
}

func (sp *SelectPlan) min(a, b int) int {
	if a <= b {
		return a
	}

	return b
}

func (sp *SelectPlan) DistinctVals(fld_name string) int  {
	if sp.pred.EquateWithConstant(fld_name) != nil {
		//如果查询是 A=c 类型，A 是字段，c 是常量，那么查询结果返回一条数据
		return 1
	} else {
		//如果查询是 A=B 类型，A,B 都是字段，那么查询结果返回不同类型数值较小的那个字段
		fld_name2 := sp.pred.EquateWithField(fld_name)
		if fld_name2 != "" {
			return sp.min(sp.p.DistinctVals(fld_name), sp.p.DistinctVals(fld_name2))
		} else {
			return sp.p.DistinctVals(fld_name)
		}
	}
}

func (sp *SelectPlan) Schema() e_rm.SchemaInterface {
	return sp.p.Schema()
}

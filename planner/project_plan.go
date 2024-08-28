package planner

import (
	e_rm "entry_record_manager"
	"query"
)

type ProjectPlan struct {
	p      Plan
	schema *e_rm.Schema
}

func NewProjectPlan(p Plan, fld_list []string) *ProjectPlan {
	project_plan := ProjectPlan{
		p:      p,
		schema: e_rm.NewSchema(),
	}

	for _, fld := range fld_list {
		project_plan.schema.Add(fld, project_plan.p.Schema())
	}

	return &project_plan
}

func (pjp *ProjectPlan) Open() interface{} {
	scan := pjp.p.Open()
	return query.NewProjectScan(scan.(query.Scan), pjp.schema.Fields())
}

func (pjp *ProjectPlan) BlocksAccessed() int {
	return pjp.p.BlocksAccessed()
}

func (pjp *ProjectPlan) RecordsOutput() int {
	return pjp.p.RecordsOutput()
}

func (pjp *ProjectPlan) DistinctVals(fld_name string) int {
	return pjp.p.DistinctVals(fld_name)
}

func (pjp *ProjectPlan) Schema() e_rm.SchemaInterface {
	return pjp.schema
}

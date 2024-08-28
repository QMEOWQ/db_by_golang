package planner

import (
	e_rm "entry_record_manager"
	"query"
)

type ProductPlan struct {
	p1     Plan
	p2     Plan
	schema *e_rm.Schema
}

func NewProductPlan(p1 Plan, p2 Plan) *ProductPlan {
	product_plan := ProductPlan{
		p1:     p1,
		p2: 	p2,
		schema: e_rm.NewSchema(),
	}

	product_plan.schema.AddAll(p1.Schema())
	product_plan.schema.AddAll(p2.Schema())

	return &product_plan
}

func (pdp *ProductPlan) Open() interface{} {
	scan1 := pdp.p1.Open()
	scan2 := pdp.p2.Open()
	return query.NewProductScan(scan1.(query.Scan), scan2.(query.Scan))
}

func (pdp *ProductPlan) BlocksAccessed() int {
	return pdp.p1.BlocksAccessed() + (pdp.p1.RecordsOutput() * pdp.p2.BlocksAccessed())
}

func (pdp *ProductPlan) RecordsOutput() int {
	return pdp.p1.RecordsOutput() * pdp.p2.RecordsOutput()
}

func (pdp *ProductPlan) DistinctVals(fld_name string) int {
	if pdp.p1.Schema().HasFields(fld_name) {
		return pdp.p1.DistinctVals(fld_name)
	} else {
		return pdp.p2.DistinctVals(fld_name)
	}
}

func (pdp *ProductPlan) Schema() e_rm.SchemaInterface {
	return pdp.schema
}



package query

import (
	e_rm "entry_record_manager"
	"math"
)

// term -> expr op expr

type Term struct {
	left  *Expression
	right *Expression
}

func NewTerm(left *Expression, right *Expression) *Term {
	return &Term{
		left,
		right,
	}
}

func (t *Term) IsSatisfied(s Scan) bool {
	l_val := t.left.Evaluate(s)
	r_val := t.right.Evaluate(s)
	return r_val.Equals(l_val)
}

func (t *Term) ApplyTo(sch *e_rm.Schema) bool {
	return t.left.ApplyTo(sch) && t.right.ApplyTo(sch)
}

func (t *Term) ReductionFactor(p Plan) int {
	left_name := ""
	right_name := ""
	if t.left.IsFieldName() && t.right.IsFieldName() {
		left_name = t.left.AsFieldName()
		right_name = t.right.AsFieldName()
		if p.DistinctVals(left_name) > p.DistinctVals(right_name) {
			return p.DistinctVals(left_name)
		}
		return p.DistinctVals(right_name)
	}

	if t.left.IsFieldName() {
		left_name = t.left.AsFieldName()
		return p.DistinctVals(left_name)
	}

	if t.right.IsFieldName() {
		right_name = t.right.AsFieldName()
		return p.DistinctVals(right_name)
	}

	if t.left.AsConstant().Equals(t.right.AsConstant()) {
		return 1
	} else {
		return math.MaxInt
	}
}

func (t *Term) EquateWithConstant(fld_name string) *Constant {
	if t.left.IsFieldName() && t.left.AsFieldName() == fld_name && !t.right.IsFieldName() {
		return t.right.AsConstant()
	} else if t.right.IsFieldName() && t.right.AsFieldName() == fld_name && !t.left.IsFieldName() {
		return t.left.AsConstant()
	} else {
		return nil
	}
}

func (t *Term) EquateWithField(fld_name string) string {
	if t.left.IsFieldName() && t.left.AsFieldName() == fld_name && t.right.IsFieldName() {
		return t.right.AsFieldName()
	} else if t.right.IsFieldName() && t.right.AsFieldName() == fld_name && t.left.IsFieldName() {
		return t.left.AsFieldName()
	}

	return ""
}

func (t *Term) ToString() string {
	return t.left.ToString() + "=" + t.right.ToString()
}

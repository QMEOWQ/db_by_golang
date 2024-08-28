package query

import (
	e_rm "entry_record_manager"
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

func (t *Term) ToString() string {
	return t.left.ToString() + "=" + t.right.ToString()
}


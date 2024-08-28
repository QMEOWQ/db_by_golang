package query

import (
	e_rm "entry_record_manager"
)

type Predicate struct {
	terms []*Term
}

func NewPredicate() *Predicate {
	return &Predicate{}
}

func NewPredicateWithTerms(t *Term) *Predicate {
	predicate := &Predicate{}
	predicate.terms = make([]*Term, 0)
	predicate.terms = append(predicate.terms, t)
	return predicate
}

func (p *Predicate) ConjoinWith(pred *Predicate) {
	p.terms = append(p.terms, pred.terms...)
}

func (p *Predicate) IsSatisfied(s Scan) bool {
	for _, t := range p.terms {
		if !t.IsSatisfied(s) {
			return false
		}
	}

	return true
}

func (p *Predicate) ReductionFactor(plan Plan) int {
	factor := 1
	for _, term := range p.terms {
		factor *= term.ReductionFactor(plan)
	}

	return factor
}

func (p *Predicate) SelectSubPred(sch *e_rm.Schema) *Predicate {
	res := NewPredicate()
	for _, term := range p.terms {
		if term.ApplyTo(sch) {
			res.terms = append(res.terms, term)
		}
	}

	if len(res.terms) == 0 {
		return nil
	}

	return res
}

func (p *Predicate) JoinSubPred(sch1 *e_rm.Schema, sch2 *e_rm.Schema) *Predicate {
	res := NewPredicate()

	new_sch := e_rm.NewSchema()
	new_sch.AddAll(sch1)
	new_sch.AddAll(sch2)

	for _, term := range p.terms {
		if !term.ApplyTo(sch1) && !term.ApplyTo(sch2) && term.ApplyTo(new_sch) {
			res.terms = append(res.terms, term)
		}
	}

	if len(res.terms) == 0 {
		return nil
	}

	return res
}

func (p *Predicate) EquateWithConstant(fld_name string) *Constant {
	for _, term := range p.terms {
		c := term.EquateWithConstant(fld_name)
		if c != nil {
			return c
		}
	}

	return nil
}

func (p *Predicate) EquateWithField(fld_name string) string {
	for _, term := range p.terms {
		s :=  term.EquateWithField(fld_name)
		if s != "" {
			return s
		}
	}

	return ""
}

func (p *Predicate) ToString() string {
	res := ""
	for _, t := range p.terms {
		res += " and " + t.ToString()
	}

	//return res
	return res[5:]
}

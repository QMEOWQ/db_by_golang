package query

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

func (p *Predicate) ToString() string {
	res := ""
	for _, t := range p.terms {
		res += " and " + t.ToString()
	}

	//return res
	return res[5:]
}

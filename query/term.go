package query


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

func (t *Term) ToString() string {
	return t.left.ToString() + "=" + t.right.ToString()
}

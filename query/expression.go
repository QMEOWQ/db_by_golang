package query

import (
	e_rm "entry_record_manager"
)

//expr -> field | constant

type Expression struct {
	val      *Constant
	fld_name string
}

func NewExpressionWithConstant(val *Constant) *Expression {
	return &Expression{
		val:      val,
		fld_name: "",
	}
}

func NewExpressionWithFldName(fld_name string) *Expression {
	return &Expression{
		val:      nil,
		fld_name: fld_name,
	}
}

func (e *Expression) IsFieldName() bool {
	return e.fld_name != ""
}

func (e *Expression) AsConstant() *Constant {
	return e.val
}

func (e *Expression) AsFieldName() string {
	return e.fld_name
}

func (e *Expression) Evaluate(s Scan) *Constant {
	//expression 可能对应一个常量，或一个字段名
	if e.val != nil {
		return e.val
	}

	return s.GetVal(e.fld_name)
}

func (e *Expression) ApplyTo(sch *e_rm.Schema) bool {
	if e.val != nil {
		return true
	}

	return sch.HasFields(e.fld_name)
}

func (e *Expression) ToString() string {
	if e.val != nil {
		return e.val.ToString()
	}

	return e.fld_name
}

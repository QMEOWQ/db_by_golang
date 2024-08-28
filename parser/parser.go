package parser

import (
	"fmt"
	"lexer"
	"query"
	"strconv"
	"strings"
)

type SqlParser struct {
	sqlLexer lexer.Lexer
}

func NewSqlParser(sql string) *SqlParser {
	return &SqlParser{
		sqlLexer: lexer.NewLexer(sql),
	}
}

//自顶向下处理

/*
1. 对以下语法进行解析
	FIELD -> ID
	CONSTANT -> STRING | NUM
	EXPRESSION -> FIELD | CONSTANT
	TERM -> EXPRESSION EQ EXPRESSION
*/

func (sp *SqlParser) Field() (lexer.Token, string) {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag != lexer.ID {
		panic("Tag of FIELD is not ID.")
	}

	return tok, sp.sqlLexer.Lexeme
}

func (sp *SqlParser) Constant() *query.Constant {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	switch tok.Tag {

	case lexer.STRING:
		s := strings.Clone(sp.sqlLexer.Lexeme)
		return query.NewConstantWithString(&s)
		break

	case lexer.NUM:
		num, err := strconv.Atoi(sp.sqlLexer.Lexeme)
		if err != nil {
			//panic(err)
			panic("string is not a number!")
		}
		return query.NewConstantWithInt(&num)
		break
	default:
		panic("token is not a string or number identifier!")
	}

	return nil
}

func (sp *SqlParser) Expression() *query.Expression {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == lexer.ID {
		sp.sqlLexer.ReverseScan()
		_, str := sp.Field()
		return query.NewExpressionWithFldName(str)
	} else {
		sp.sqlLexer.ReverseScan()
		constant := sp.Constant()
		return query.NewExpressionWithConstant(constant)
	}
}

func (sp *SqlParser) Term() *query.Term {
	left := sp.Expression()

	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != lexer.ASSIGN_OPERATOR {
		panic("Tag of TERM is not ASSIGN_OPERATOR = .")
	}

	right := sp.Expression()

	return query.NewTerm(left, right)
}

/*
 2. 对select语句进行解析
    PREDICATE -> TERM (AND PREDICATE)?
    QUERY -> SELECT SELECT_LIST FROM TABLE_LIST (WHERE PREDICATE)?
    SELECT_LIST -> FIELD (COMMA SELECTION_LIST)?
    TABLE_LIST -> ID (COMMA TABLE_LIST)?
*/

func (sp *SqlParser) Predicate() *query.Predicate {
	//PREDICATE -> TERM (AND PREDICATE)? (where后的条件部分)
	pred := query.NewPredicateWithTerms(sp.Term())

	tok, err := sp.sqlLexer.Scan()
	if err != nil && fmt.Sprint(err) != "EOF" {
		panic(err)
	}

	if tok.Tag == lexer.AND {
		pred.ConjoinWith(sp.Predicate())
	} else {
		sp.sqlLexer.ReverseScan()
	}

	return pred
}

func (sp *SqlParser) Query() *QueryData {
	//QUERY -> SELECT SELECT_LIST FROM TABLE_LIST (WHERE PREDICATE)?
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag != lexer.SELECT {
		panic("token is not select")
	}

	fields := sp.SelectList()

	tok, err = sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag != lexer.FROM {
		panic("token is not from")
	}

	tables := sp.TableList()

	tok, err = sp.sqlLexer.Scan()
	pred := query.NewPredicate()
	if err == nil && tok.Tag == lexer.WHERE {
		pred = sp.Predicate()
	} else {
		sp.sqlLexer.ReverseScan()
	}

	return NewQueryData(fields, tables, pred)
}

func (sp *SqlParser) SelectList() []string {
	//SELECT_LIST -> FIELD (COMMA SELECTION_LIST)?
	fields := make([]string, 0)
	_, field := sp.Field()
	fields = append(fields, field)

	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == lexer.COMMA {
		//将select多列
		selectList := sp.SelectList()
		fields = append(fields, selectList...)
	} else {
		sp.sqlLexer.ReverseScan()
	}

	return fields
}

func (sp *SqlParser) TableList() []string {
	//TABLE_LIST -> ID (COMMA TABLE_LIST)? ID实际是表名
	tables := make([]string, 0)
	// _, table := sp.Field()
	// tables = append(tables, table)

	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag != lexer.ID {
		panic("token is not a table name")
	}

	tables = append(tables, sp.sqlLexer.Lexeme)

	tok, err = sp.sqlLexer.Scan()
	if err == nil && tok.Tag == lexer.COMMA {
		tableList := sp.TableList()	
		tables = append(tables, tableList...)
	} else {
		sp.sqlLexer.ReverseScan()
	}

	return tables
}

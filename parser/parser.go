package parser

import (
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

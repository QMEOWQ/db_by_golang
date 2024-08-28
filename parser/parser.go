package parser

import (
	e_rm "entry_record_manager"
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

// 辅助函数
func (sp *SqlParser) checkWordTag(wordTag lexer.Tag) {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != wordTag {
		panic("token is not a match.")
	}
}

func (sp *SqlParser) isMatchTag(wordTag lexer.Tag) bool {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == wordTag {
		return true
	} else {
		sp.sqlLexer.ReverseScan()
		return false
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

// 列名
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

/*
3. create 语句解析
	CREATE_COMMAND -> CREATE_TABLE | CREATE_VIEW | CREATE_INDEX
	CREATE_TABLE -> CREATE TABLE FIELD_DEFS
	FIELD_DEFS -> FIELD_DEF (COMMA FIELD_DEFS)?
	FIELD_DEF -> ID TYPE_DEF
	TYPE_DEF -> INT | VARCHAR LEFT_BRACE NUM RIGHT_BRACE

	CREATE_VIEW -> CREATE VIEW ID AS QUERY

	CREATE_INDEX -> CREATE INDEX ID ON ID LEFT_BRACE FIELD
*/

func (sp *SqlParser) UpdateCmd() interface{} {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == lexer.INSERT {
		sp.sqlLexer.ReverseScan()
		//return sp.Insert()
	} else if tok.Tag == lexer.DELETE {
		sp.sqlLexer.ReverseScan()
		//return sp.Delete()
	} else if tok.Tag == lexer.UPDATE {
		sp.sqlLexer.ReverseScan()
		//return sp.Modify()
	} else {
		sp.sqlLexer.ReverseScan()
		return sp.Create()
	}

	return nil
}

func (sp *SqlParser) Create() interface{} {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != lexer.CREATE {
		panic("token is not create!")
	}

	tok, err = sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == lexer.TABLE {
		return sp.CreateTable()
	} else if tok.Tag == lexer.VIEW {
		return sp.CreateView()
	} else if tok.Tag == lexer.INDEX {
		return sp.CreateIndex()
	}

	panic("sql string with create should not end here!")
}

func (sp *SqlParser) CreateTable() interface{} {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	//表名
	if tok.Tag != lexer.ID {
		panic("token is not a table name!")
	}

	tbl_name := sp.sqlLexer.Lexeme

	tok, err = sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != lexer.LEFT_BRACKET {
		panic("missing left bracket!")
	}

	//表中字段的组织结构
	sch := sp.FieldDefs()

	tok, err = sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != lexer.RIGHT_BRACKET {
		panic("missing right bracket!")
	}

	//return NewCreateTabLeData(tbl_name, sch)

	table_data := NewCreateTabLeData(tbl_name, sch)
	table_data_def := fmt.Sprintf("table def : %s", table_data.ToString())
	fmt.Println(table_data_def)

	return table_data
}

func (sp *SqlParser) FieldDefs() *e_rm.Schema {
	sch := sp.FieldDef()

	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag == lexer.COMMA {
		sch2 := sp.FieldDefs()
		sch.AddAll(sch2) //合并schema
	} else {
		sp.sqlLexer.ReverseScan()
	}

	return sch
}

func (sp *SqlParser) FieldDef() *e_rm.Schema {
	_, field_name := sp.Field()
	return sp.FieldType(field_name)
}

func (sp *SqlParser) FieldType(field_name string) *e_rm.Schema {
	sch := e_rm.NewSchema()

	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == lexer.INT {
		sch.AddIntField(field_name)
	} else if tok.Tag == lexer.VARCHAR {
		tok, err := sp.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if tok.Tag != lexer.LEFT_BRACKET {
			panic("missing left bracket!")
		}

		tok, err = sp.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if tok.Tag != lexer.NUM {
			panic("it is not number for varchar length!")
		}

		varchar_len := sp.sqlLexer.Lexeme
		fld_len, err := strconv.Atoi(varchar_len)
		if err != nil {
			panic(err)
		}
		sch.AddStringField(field_name, fld_len)

		tok, err = sp.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if tok.Tag != lexer.RIGHT_BRACKET {
			panic("missing right bracket!")
		}
	}

	return sch
}

func (sp *SqlParser) CreateView() interface{} {
	sp.checkWordTag(lexer.ID)

	view_name := sp.sqlLexer.Lexeme

	sp.checkWordTag(lexer.AS)

	query_data := sp.Query()

	view_data := NewViewData(view_name, query_data)
	view_data_def := fmt.Sprintf("vd def : %s", view_data.ToString())
	fmt.Println(view_data_def)

	return view_data
}

func (sp *SqlParser) CreateIndex() interface{} {
	sp.checkWordTag(lexer.ID)

	idx_name := sp.sqlLexer.Lexeme

	sp.checkWordTag(lexer.ON)
	sp.checkWordTag(lexer.ID)

	tbl_name := sp.sqlLexer.Lexeme

	sp.checkWordTag(lexer.LEFT_BRACKET)
	_, fld_name := sp.Field()
	sp.checkWordTag(lexer.RIGHT_BRACKET)

	idx_data := NewIndexData(idx_name, tbl_name, fld_name)
	fmt.Printf("create index def : %s", idx_data.ToString())

	return idx_data
}

/*
4. insert 语句解析
*/


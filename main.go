package main

// import (
// 	bm "buffer_manager"
// 	e_rm "entry_record_manager"
// 	fm "file_manager"
// 	"fmt"
// 	lm "log_manager"
// 	ts "transaction"
// 	mdm "metadata_manager"

// 	"math/rand"
// )

import (
	"fmt"
	"parser"
)

func main() {
	// select test
	SqlParser := parser.NewSqlParser("select age, name, sex from student, department where age = 20 and sex = \"male\" ")
	query_data := SqlParser.Query()
	fmt.Println(query_data.ToString())

	//parser test
	// SqlParser := parser.NewSqlParser("age = 22")
	// term := SqlParser.Term()
	// str := fmt.Sprintf("term: %v", term)
	// fmt.Println(str)

	// lexer test
	// sqlLexer := lexer.NewLexer("select name , sex from student where age > 22")
	// var tokens []*lexer.Token
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.SELECT, "select"))
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.ID, "name"))
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.COMMA, ","))
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.ID, "sex"))
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.FROM, "from"))
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.ID, "student"))
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.WHERE, "where"))
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.ID, "age"))
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.GREATER_OPERATOR, ">"))
	// tokens = append(tokens, lexer.NewTokenWithString(lexer.NUM, "20"))

	// for _, tok := range tokens {
	// 	sqlTok, err := sqlLexer.Scan()
	// 	if err != nil {
	// 		fmt.Println("lexer error")
	// 		break
	// 	}

	// 	if sqlTok.Tag != tok.Tag {
	// 		err_str := fmt.Sprintf("token err, expect: %v, but got %v\n", tok, sqlTok)
	// 		fmt.Println(err_str)
	// 		break
	// 	}
	// }

	// fmt.Println("lexer testing pass...")
}

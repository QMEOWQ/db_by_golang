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
	"parser"
)

func main() {
	//update/modify test
	sql := "UPDATE Customers SET City=\"Berlin\" WHERE CustomerID=1"
	sqlParser := parser.NewSqlParser(sql)
	sqlParser.UpdateCmd() //流程走完没有报错并打印出相关信息即解析成功


	//delete test 
	// sql := "DELETE FROM Customers WHERE CustomerName=\"Alfreds Futterkiste\""
	// sqlParser := parser.NewSqlParser(sql)
	// sqlParser.UpdateCmd()

	//insert test
	// sql := "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) " +
	// 	"VALUES (\"Cardinal\", \"Tom B. Erichsen\", \"Skagen 21\", \"Stavanger\", 4006, \"Norway\")"
	// sqlParser := parser.NewSqlParser(sql)
	// sqlParser.UpdateCmd() //流程走完没有报错并打印出相关信息即解析成功

	//create test
	// sql := "create table person (PersonID int, LastName varchar(255), FirstName varchar(255)," +
	// 	"Address varchar(255), City varchar(255) )"
	// SqlParser := parser.NewSqlParser(sql)
	// SqlParser.UpdateCmd() //流程走完没有报错并打印出相关信息即解析成功

	// sql := "create view Customer as select CustomerName, ContactName from customers where country=\"China\""
	// sqlParser := parser.NewSqlParser(sql)
	// sqlParser.UpdateCmd() //流程走完没有报错并打印出相关信息即解析成功

	//sql := "create index idx_lastName on persons (lastname)"
	// sql := "create index idxLastName on persons (lastname)"
	// SqlParser := parser.NewSqlParser(sql)
	// SqlParser.UpdateCmd() //流程走完没有报错并打印出相关信息即解析成功

	// select test
	// SqlParser := parser.NewSqlParser("select age, name, sex from student, department where age = 20 and sex = \"male\" ")
	// query_data := SqlParser.Query()
	// fmt.Println(query_data.ToString())

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

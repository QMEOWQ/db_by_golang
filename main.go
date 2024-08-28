package main

import (
	bm "buffer_manager"
	//e_rm "entry_record_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	ts "transaction"
	mdm "metadata_manager"
	"planner"
	"query"
	"parser"
)

//index test
func PrintStudentTable(ts *ts.Transaction, mdm *mdm.MetaDataManager) {
	queryStr := "select name, majorid, gradyear from STUDENT"
	p := parser.NewSqlParser(queryStr)
	queryData := p.Query()
	test_planner := planner.NewBasicQueryPlanner(mdm)
	test_plan := test_planner.CreatePlan(queryData, ts)
	test_interface := (test_plan.Open())
	test_scan, _ := test_interface.(query.Scan)
	for test_scan.Next() {
		fmt.Printf("name: %s, majorid: %d, gradyear: %d\n",
			test_scan.GetString("name"), test_scan.GetInt("majorid"),
			test_scan.GetInt("gradyear"))
	}
}

func CreateInsertUpdateByUpdatePlanner() {
	file_manager, _ := fm.NewFileManager("student", 2048)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)
	ts := ts.NewTransaction(file_manager, log_manager, buffer_manager)
	mdm := mdm.NewMetaDataManager(file_manager.Is_dir_new(), ts)

	updatePlanner := planner.NewBasicUpdatePlanner(mdm)
	createTableSql := "create table STUDENT (name varchar(16), majorid int, gradyear int)"
	p := parser.NewSqlParser(createTableSql)
	tableData := p.UpdateCmd().(*parser.CreateTableData)
	updatePlanner.ExecuteCreateTable(tableData, ts)

	insertSQL := "insert into STUDENT (name, majorid, gradyear) values(\"tylor\", 30, 2020)"
	p = parser.NewSqlParser(insertSQL)
	insertData := p.UpdateCmd().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, ts)
	insertSQL = "insert into STUDENT (name, majorid, gradyear) values(\"tom\", 35, 2023)"
	p = parser.NewSqlParser(insertSQL)
	insertData = p.UpdateCmd().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, ts)

	fmt.Println("table after insert:")
	PrintStudentTable(ts, mdm)

	updateSQL := "update STUDENT set majorid=20 where majorid=30 and gradyear=2020"
	p = parser.NewSqlParser(updateSQL)
	updateData := p.UpdateCmd().(*parser.ModifyData)
	updatePlanner.ExecuteModify(updateData, ts)

	fmt.Println("table after update:")
	PrintStudentTable(ts, mdm)

	deleteSQL := "delete from STUDENT where majorid=35"
	p = parser.NewSqlParser(deleteSQL)
	deleteData := p.UpdateCmd().(*parser.DeleteData)
	updatePlanner.ExecuteDelete(deleteData, ts)

	fmt.Println("table after delete")
	PrintStudentTable(ts, mdm)
}

func TestIndex() {
	file_manager, _ := fm.NewFileManager("student", 4096)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)
	ts := ts.NewTransaction(file_manager, log_manager, buffer_manager)
	fmt.Printf("file manager is new: %v\n", file_manager.Is_dir_new())
	mdm := mdm.NewMetaDataManager(file_manager.Is_dir_new(), ts)

	//创建 student 表，并插入一些记录
	updatePlanner := planner.NewBasicUpdatePlanner(mdm)
	createTableSql := "create table STUDENT (name varchar(16), majorid int, gradyear int)"
	p := parser.NewSqlParser(createTableSql)
	tableData := p.UpdateCmd().(*parser.CreateTableData)
	updatePlanner.ExecuteCreateTable(tableData, ts)

	insertSQL := "insert into STUDENT (name, majorid, gradyear) values(\"tylor\", 30, 2020)"
	p = parser.NewSqlParser(insertSQL)
	insertData := p.UpdateCmd().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, ts)
	insertSQL = "insert into STUDENT (name, majorid, gradyear) values(\"tom\", 35, 2023)"
	p = parser.NewSqlParser(insertSQL)
	insertData = p.UpdateCmd().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, ts)

	fmt.Println("table after insert:")
	PrintStudentTable(ts, mdm)
	//在 student 表的 majorid 字段建立索引
	mdm.CreateIndex("majoridIdx", "STUDENT", "majorid", ts)
	//查询建立在 student 表上的索引并根据索引输出对应的记录信息
	studetPlan := planner.NewTablePlan(ts, "STUDENT", mdm)
	updateScan := studetPlan.Open().(*query.TableScan)
	//先获取每个字段对应的索引对象,这里我们只有 majorid 建立了索引对象
	indexes := mdm.GetIndexInfo("STUDENT", ts)
	//获取 majorid 对应的索引对象
	majoridIdxInfo := indexes["majorid"]
	//将改rid 加入到索引表
	majorIdx := majoridIdxInfo.Open()
	updateScan.FirstQualify()
	for updateScan.Next() {
		//返回当前记录的 rid
		dataRID := updateScan.GetRID()
		dataVal := updateScan.GetVal("majorid")
		majorIdx.Insert(dataVal, dataRID)
	}

	//通过索引表获得给定字段内容的记录
	majorid := 35
	majorIdx.FirstQualify(query.NewConstantWithInt(&majorid))
	for majorIdx.Next() {
		datarid := majorIdx.GetDataRID()
		updateScan.MoveToRid(datarid)
		fmt.Printf("student name :%s, id: %d\n", updateScan.GetScan().GetString("name"),
			updateScan.GetScan().GetInt("majorid"))
	}

	majorIdx.Close()
	updateScan.GetScan().Close()
	ts.Commit()

}

func main() {
	TestIndex()
}

//planner test 2
// func createStudentTable() (*ts.Transaction, *mdm.MetaDataManager) {
// 	file_manager, _ := fm.NewFileManager("student", 2048)
// 	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
// 	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)
// 	ts := ts.NewTransaction(file_manager, log_manager, buffer_manager)
// 	sch := e_rm.NewSchema()
// 	mdm := mdm.NewMetaDataManager(false, ts)

// 	sch.AddStringField("name", 16)
// 	sch.AddIntField("id")
// 	layout := e_rm.NewLayoutWithSchema(sch)

// 	tbs := query.NewTableScan(ts, "student", layout)
// 	tbs.FirstQualify()
// 	for i := 1; i <= 3; i++ {
// 		tbs.Insert() //指向一个可用插槽
// 		tbs.SetInt("id", i)
// 		if i == 1 {
// 			tbs.SetString("name", "Tom")
// 		}
// 		if i == 2 {
// 			tbs.SetString("name", "Jim")
// 		}
// 		if i == 3 {
// 			tbs.SetString("name", "John")
// 		}
// 	}

// 	mdm.CreateTable("student", sch, ts)

// 	exam_sch := e_rm.NewSchema()

// 	exam_sch.AddIntField("stuid")
// 	exam_sch.AddStringField("exam", 16)
// 	exam_sch.AddStringField("grad", 16)
// 	exam_layout := e_rm.NewLayoutWithSchema(exam_sch)

// 	tbs = query.NewTableScan(ts, "exam", exam_layout)
// 	tbs.FirstQualify()

// 	tbs.Insert() //指向一个可用插槽
// 	tbs.SetInt("stuid", 1)
// 	tbs.SetString("exam", "math")
// 	tbs.SetString("grad", "A")

// 	tbs.Insert() //指向一个可用插槽
// 	tbs.SetInt("stuid", 1)
// 	tbs.SetString("exam", "algorithm")
// 	tbs.SetString("grad", "B")

// 	tbs.Insert() //指向一个可用插槽
// 	tbs.SetInt("stuid", 2)
// 	tbs.SetString("exam", "writing")
// 	tbs.SetString("grad", "C")

// 	tbs.Insert() //指向一个可用插槽
// 	tbs.SetInt("stuid", 2)
// 	tbs.SetString("exam", "physics")
// 	tbs.SetString("grad", "C")

// 	tbs.Insert() //指向一个可用插槽
// 	tbs.SetInt("stuid", 3)
// 	tbs.SetString("exam", "chemical")
// 	tbs.SetString("grad", "B")

// 	tbs.Insert() //指向一个可用插槽
// 	tbs.SetInt("stuid", 3)
// 	tbs.SetString("exam", "english")
// 	tbs.SetString("grad", "C")

// 	mdm.CreateTable("exam", exam_sch, ts)

// 	return ts, mdm
// }

// func main() {
// 	//构造 student 表
// 	ts, mdm := createStudentTable()
// 	queryStr := "select name from student, exam where id = stuid and grad=\"A\""
// 	p := parser.NewSqlParser(queryStr)
// 	queryData := p.Query()
// 	test_planner := planner.NewBasicQueryPlanner(mdm)
// 	test_plan := test_planner.CreatePlan(queryData, ts)
// 	test_interface := (test_plan.Open())
// 	test_scan, _ := test_interface.(query.Scan)
// 	for test_scan.Next() {
// 		fmt.Printf("name: %s\n", test_scan.GetString("name"))
// 	}

// }

// planner test 1
// func printStats(n int, p planner.Plan) {
// 	fmt.Printf("Here are statbs for plan p %d\n", n)
// 	fmt.Printf("\tB(p%d):%d\n", n, p.BlocksAccessed())
// 	fmt.Printf("\tR(p%d):%d\n", n, p.RecordsOutput())
// }

// func createStudentTable() (*tbs.Transaction, *mdm.MetaDataManager)  {
// 	file_manager, _ := fm.NewFileManager("student", 4096)
// 	log_manager, _ := lm.NewLogManager(file_manager, "planner_logfile.log")
// 	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)

// 	tbs := tbs.NewTransaction(file_manager, log_manager, buffer_manager)
// 	sch := e_rm.NewSchema()

// 	sch.AddStringField("sname", 16)
// 	sch.AddIntField("majorID")
// 	sch.AddIntField("gradyear")

// 	layout := e_rm.NewLayoutWithSchema(sch)
// 	for _, fld_name := range layout.Schema().Fields() {
// 		offset := layout.Offset(fld_name)
// 		fmt.Printf("%s has offset %d\n", fld_name, offset)
// 	}

// 	tbs := query.NewTableScan(tbs, "student", layout)
// 	fmt.Println("Filling the table with 50 random records")
// 	//tbs.BeforeFirst()
// 	tbs.FirstQualify()
// 	val_for_fld_sname := make([]int, 0)
// 	for i := 0; i < 50; i++ {
// 		tbs.Insert() //指向一个可用插槽
// 		tbs.SetInt("majorID", i) 
// 		tbs.SetInt("gradyear", 2000 + i)
// 		val_for_fld_sname = append(val_for_fld_sname, i)

// 		str := fmt.Sprintf("sname_%d", i)
// 		tbs.SetString("sname", str)
// 		fmt.Printf("Inserting into slot %s : {%d , %s}\n ", tbs.GetRID().ToString(), i, str)		
// 	}

// 	mdm := mdm.NewMetaDataManager(false, tbs)
// 	mdm.CreateTable("student", sch, tbs)

// 	return tbs, mdm
// }

// func main() {
// 	//构造student table
// 	tbs, mdm := createStudentTable()
// 	p1 := planner.NewTablePlan(tbs, "student", mdm)
// 	n := 10
// 	t := query.NewTerm(query.NewExpressionWithFldName("majorId"),
// 		query.NewExpressionWithConstant(query.NewConstantWithInt(&n)))
// 	pred := query.NewPredicateWithTerms(t)
// 	p2 := planner.NewSelectPlan(p1, pred)

// 	n1 := 2000
// 	t2 := query.NewTerm(query.NewExpressionWithFldName("gradyear"),
// 		query.NewExpressionWithConstant(query.NewConstantWithInt(&n1)))
// 	pred2 := query.NewPredicateWithTerms(t2)
// 	p3 := planner.NewSelectPlan(p1, pred2)

// 	c := make([]string, 0)
// 	c = append(c, "sname")
// 	c = append(c, "majorId")
// 	c = append(c, "gradyear")
// 	p4 := planner.NewProjectPlan(p3, c)

// 	printbstatbs(1, p1)
// 	printbstatbs(2, p2)
// 	printbstatbs(3, p3)
// 	printbstatbs(4, p4)
// }

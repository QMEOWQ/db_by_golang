package metadata_manager

// //put it int the main.go file and run.
// package main

// import (
// 	bm "buffer_manager"
// 	fm "file_manager"
// 	"fmt"
// 	lm "log_manager"
// 	mdm "metadata_manager"
// 	record_mgr "entry_record_manager"
// 	ts "transaction"
// )

// func main() {
// 	file_manager, _ := fm.NewFileManager("recordtest", 400)
// 	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
// 	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)

// 	ts := ts.NewTransaction(file_manager, log_manager, buffer_manager)
// 	sch := record_mgr.NewSchema()
// 	sch.AddIntField("A")
// 	sch.AddStringField("B", 9)

// 	tm := mdm.NewTableManager(true, ts)
// 	tm.CreateTable("MyTable", sch, ts)
// 	layout := tm.GetLayout("MyTable", ts)
// 	size := layout.SlotSize()
// 	sch2 := layout.Schema()
// 	fmt.Printf("MyTable has slot size: %d\n", size)
// 	fmt.Println("Its fields are: ")
// 	for _, fldName := range sch2.Fields() {
// 		fldType := ""
// 		if sch2.Type(fldName) == record_mgr.INTEGER {
// 			fldType = "int"
// 		} else {
// 			strlen := sch2.Length(fldName)
// 			fldType = fmt.Sprintf("varchar( %d )", strlen)
// 		}
// 		fmt.Printf("%s : %s\n", fldName, fldType)
// 	}

// 	ts.Commit()

// }
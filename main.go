package main

import (
	bm "buffer_manager"
	e_rm "entry_record_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	ts "transaction"
	mdm "metadata_manager"

	"math/rand"
)

// schema & layout & view_manager test
func main() {
	file_manager, _ := fm.NewFileManager("view_test", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "view_test_logfile.log")
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)

	ts := ts.NewTransaction(file_manager, log_manager, buffer_manager)
	sch := e_rm.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 11)

	mdm := mdm.NewMetaDataManager(true, ts)
	mdm.CreateTable("MyTable", sch, ts)
	layout := mdm.GetLayout("MyTable", ts)
	size := layout.SlotSize()
	fmt.Printf("MyTable has slot size: %d\n", size)
	sch2 := layout.Schema()
	fmt.Println("Its fields are: ")
	for _, fldName := range sch2.Fields() {
		fldType := ""
		if sch2.Type(fldName) == e_rm.INTEGER {
			fldType = "int"
		} else {
			strlen := sch2.Length(fldName)
			fldType = fmt.Sprintf("varchar ( %d )", strlen)
		}

		fmt.Printf("%s :  %s\n", fldName, fldType)
	}

	tbs := e_rm.NewTableScan(ts, "MyTable", layout)
	//测试统计元数据
	for i := 0; i < 50; i++ {
		tbs.Insert()
		n := rand.Intn(50)
		tbs.SetInt("A", n)
		strField := fmt.Sprintf("rec%d", n)
		tbs.SetString("B", strField)
	}
	si := mdm.GetStatInfo("MyTable", layout, ts)
	fmt.Printf("blocks for MyTable is %d\n", si.BlockAccessed())
	fmt.Printf("records for MyTable is :%d\n", si.RecordsOutput())
	fmt.Printf("Distinc values for field A is %d\n", si.DistinctVals("A"))
	fmt.Printf("Distinc values for field B is %d\n", si.DistinctVals("B"))

	//统计视图信息
	viewDef := "select B from MyTable where A = 1"
	mdm.CreateView("viewA", viewDef, ts)
	v := mdm.GetViewDef("viewA", ts)
	fmt.Printf("View def = %s\n", v)
	ts.Commit()
}

//lock_table test

// func main() {
// 	blk := fm.NewBlockID("lock_test_file", 1)
// 	var err_arr []error
// 	var err_arr_lock sync.Mutex
// 	lock_table := ts.NewLockTable()
// 	lock_table.XLock(blk)
// 	var wg sync.WaitGroup

// 	for i := 0; i < 3; i++ {
// 		go func(i int) {
// 			fmt.Sprintf("routine %d start\n", i)

// 			wg.Add(1)
// 			defer wg.Done()

// 			err_arr_lock.Lock()
// 			defer err_arr_lock.Unlock()

// 			err := lock_table.SLock(blk)
// 			if err == nil {
// 				fmt.Printf("access slock ok.\n")
// 			} else {
// 				fmt.Printf("slock fail for %d.\n", i)
// 			}

// 			err_arr = append(err_arr, err)
// 		}(i)
// 	}

// 	time.Sleep(1 * time.Second) //让3个线程在主线程结束前可以执行
// 	lock_table.Unlock(blk)
// 	start := time.Now()
// 	wg.Wait()
// 	end := time.Now()
// 	elapsed := end.Sub(start)
// 	fmt.Sprintf("elapsed time: %s\n", elapsed)
// }

//ts test

// func main() {
// 	file_manager, _ := fm.NewFileManager("ts_test_in_main", 400)
// 	log_manager, _ := lm.NewLogManager(file_manager, "log_file")
// 	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)

// 	ts1 := ts.NewTransaction(file_manager, log_manager, buffer_manager)
// 	blk := fm.NewBlockID("ts_test_file", 1)
// 	ts1.Pin(blk)

// 	//初始内存为随机数，不进行日志记录
// 	ts1.SetInt(blk, 80, 1, false)
// 	ts1.SetString(blk, 40, "the one", false)
// 	ts1.Commit() //回滚操作会回滚至此

// 	ts2 := ts.NewTransaction(file_manager, log_manager, buffer_manager)
// 	ts2.Pin(blk)

// 	ival, _ := ts2.GetInt(blk, 80)
// 	sval, _ := ts2.GetString(blk, 40)
// 	fmt.Println("initial value at location 80 = ", ival)
// 	fmt.Println("initial value at location 40 = ", sval)

// 	new_ival := ival + 1
// 	new_sval := sval + "!!!"

// 	ts2.SetInt(blk, 80, new_ival, true)
// 	ts2.SetString(blk, 40, new_sval, true)
// 	ts2.Commit()

// 	ts3 := ts.NewTransaction(file_manager, log_manager, buffer_manager)
// 	ts3.Pin(blk)

// 	ival, _ = ts2.GetInt(blk, 80)
// 	sval, _ = ts2.GetString(blk, 40)
// 	fmt.Println("new value at location 80 = ", ival)
// 	fmt.Println("new value at location 40 = ", sval)

// 	ts3.SetInt(blk, 80, 1024, true)
// 	//ts3.SetString(blk, 40, "modf again", false)

// 	ival, _ = ts3.GetInt(blk, 80)
// 	//sval, _ = ts3.GetString(blk, 40)
// 	fmt.Println("pre-rollback ival at location 80 = ", ival)
// 	//fmt.Println("pre-rollback sval at location 40 = ", sval)
// 	ts3.RollBack()

// 	ts4 := ts.NewTransaction(file_manager, log_manager, buffer_manager)
// 	ts4.Pin(blk)
// 	ival, _ = ts4.GetInt(blk, 80)
// 	fmt.Println("post-rollback ival at location 80 = ", ival)
// 	//fmt.Println("post-rollback sval at location 40 = ", sval)
// 	ts4.Commit()
// }

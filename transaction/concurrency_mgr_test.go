package transaction

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"testing"
	"time"
)

func TestConCurrencyManager(t *testing.T) {
	file_manager, _ := fm.NewFileManager("ts_con_test", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "log_ts_con_file")
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)

	go func() {
		ts1 := NewTransaction(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockID("testfile", 1)
		blk2 := fm.NewBlockID("testfile", 2)
		ts1.Pin(blk1)
		ts1.Pin(blk2)

		fmt.Println("Ts 1: request slock 1")

		ts1.GetInt(blk1, 0)
		fmt.Println("Ts 1: receive slock1")

		time.Sleep(2 * time.Second)

		fmt.Println("Ts 1: request slock 2")
		ts1.GetInt(blk2, 0)
		fmt.Println("Ts 1: receive slock2")
		fmt.Println("Ts 1: Commit")

		ts1.Commit()
	}()

	go func() {
		time.Sleep(1 * time.Second)

		ts2 := NewTransaction(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockID("testfile", 1)
		blk2 := fm.NewBlockID("testfile", 2)
		ts2.Pin(blk1)
		ts2.Pin(blk2)

		fmt.Println("Tx B: rquest xlock 2")

		ts2.SetInt(blk2, 0, 0, false)
		fmt.Println("Tx B: receive xlock 2")

		time.Sleep(2 * time.Second)
		fmt.Println("Tx B: request slock 1")

		ts2.GetInt(blk1, 0)
		fmt.Println("Tx B: receive slock 1")
		fmt.Println("Tx B: Commit")
		ts2.Commit()
	}()

	go func() {
		time.Sleep(2 * time.Second)
		ts3 := NewTransaction(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockID("testfile", 1)
		blk2 := fm.NewBlockID("testfile", 2)
		ts3.Pin(blk1)
		ts3.Pin(blk2)
		fmt.Println("Ts 3: rquest xlock 1")

		ts3.SetInt(blk1, 0, 0, false)
		fmt.Println("Ts 3: receive xlock 1")

		time.Sleep(1 * time.Second)
		fmt.Println("Ts 3: request slock 2")

		ts3.GetInt(blk2, 0)
		fmt.Println("Ts 3: receive slock 2")
		fmt.Println("Ts 3: Commit")
		ts3.Commit()
	}()

	time.Sleep(10 * time.Second)
}

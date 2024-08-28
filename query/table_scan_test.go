package query

import (
	bm "buffer_manager"
	e_rm "entry_record_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"testing"
	ts "transaction"

	"math/rand"

	"github.com/stretchr/testify/require"
)

func TestTableScanInsertAndDelete(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)

	ts := ts.NewTransaction(file_manager, log_manager, buffer_manager)
	sch := e_rm.NewSchema()

	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := e_rm.NewLayoutWithSchema(sch)
	for _, field_name := range layout.Schema().Fields() {
		offset := layout.Offset(field_name)
		fmt.Printf("%s has offset %d\n", field_name, offset)
	}

	tbs := NewTableScan(ts, "T", layout)
	fmt.Println("Filling the table with 50 random records")
	tbs.FirstQualify()
	val_for_field_A := make([]int, 0)
	for i := 0; i < 50; i++ {
		tbs.Insert() //指向一个可用插槽
		n := rand.Intn(50)
		tbs.SetInt("A", n)
		val_for_field_A = append(val_for_field_A, n)
		s := fmt.Sprintf("rec%d", n)
		tbs.SetString("B", s)
		fmt.Printf("inserting into slot %s: {%d , %s}\n", tbs.GetRID().ToString(), n, s)
	}

	tbs.FirstQualify()
	//测试插入的内容是否正确
	slot := 0
	for tbs.Next() {
		a := tbs.GetInt("A")
		b := tbs.GetString("B")
		require.Equal(t, a, val_for_field_A[slot])
		require.Equal(t, b, fmt.Sprintf("rec%d", a))
		slot += 1
	}

	fmt.Println("Deleting records with A-values < 25")
	count := 0
	tbs.FirstQualify()
	for tbs.Next() {
		a := tbs.GetInt("A")
		b := tbs.GetString("B")
		if a < 25 {
			count += 1
			fmt.Printf("slot %s : { %d , %s}\n", tbs.GetRID().ToString(), a, b)
			tbs.Delete()
		}
	}

	fmt.Println("Here are the remaining records:")
	tbs.FirstQualify()
	for tbs.Next() {
		a := tbs.GetInt("A")
		b := tbs.GetString("B")
		require.Equal(t, a >= 25, true)
		fmt.Printf("slot %s : { %d , %s}\n", tbs.GetRID().ToString(), a, b)
	}

	tbs.Close()
	ts.Commit()
}

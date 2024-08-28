package query

import (
	e_rm "entry_record_manager"
	fm "file_manager"
	ts "transaction"
)

type TableScan struct {
	ts             *ts.Transaction
	layout         e_rm.LayoutInterface
	e_rm_interface e_rm.EntryRecordManagerInterface
	file_name      string
	current_slot   int
}

func NewTableScan(ts *ts.Transaction, table_name string, layout e_rm.LayoutInterface) *TableScan {
	table_scan := &TableScan{
		ts:        ts,
		layout:    layout,
		file_name: table_name + ".tbl",
	}

	size, err := ts.File_block_size(table_scan.file_name)
	if err != nil {
		panic(err)
	}
	if size == 0 {
		//文件为空，增加一个区块
		table_scan.MoveToNewBlock()
	} else {
		table_scan.MoveToBlock(0)
	}

	return table_scan
}

func (tbl_s *TableScan) GetScan() Scan {
	return tbl_s
}

func (tbl_s *TableScan) Close() {
	if tbl_s.e_rm_interface != nil {
		tbl_s.ts.UnPin(tbl_s.e_rm_interface.Block())
	}
}

// func (tbl_s *TableScan) BeforeFirst() {
// 	tbl_s.MoveToBlock(0)
// }

func (tbl_s *TableScan) FirstQualify() {
	tbl_s.MoveToBlock(0)
}

func (tbl_s *TableScan) Next() bool {
	//如果在当前区块找不到给定有效记录则遍历后续区块，直到所有区块都遍历为止
	tbl_s.current_slot = tbl_s.e_rm_interface.NextAfter(tbl_s.current_slot)
	for tbl_s.current_slot < 0 {
		if tbl_s.AtLastBlock() {
			//直到最后一个区块都找不到给定插槽
			return false
		}

		tbl_s.MoveToBlock(int(tbl_s.e_rm_interface.Block().Number() + 1))
		tbl_s.current_slot = tbl_s.e_rm_interface.NextAfter(tbl_s.current_slot)
	}

	return true
}

func (tbl_s *TableScan) Insert() {
	tbl_s.current_slot = tbl_s.e_rm_interface.InsertAfter(tbl_s.current_slot)
	for tbl_s.current_slot < 0 {
		//当前区块找不到可用插槽
		if tbl_s.AtLastBlock() {
			tbl_s.MoveToNewBlock()
		} else {
			tbl_s.MoveToBlock(int(tbl_s.e_rm_interface.Block().Number() + 1))
		}

		tbl_s.current_slot = tbl_s.e_rm_interface.InsertAfter(tbl_s.current_slot)
	}
}

func (tbl_s *TableScan) Delete() {
	tbl_s.e_rm_interface.Delete(tbl_s.current_slot)
}

func (tbl_s *TableScan) HasField(field_name string) bool {
	return tbl_s.layout.Schema().HasFields(field_name)
}

func (tbl_s *TableScan) GetInt(field_name string) int {
	return tbl_s.e_rm_interface.GetInt(tbl_s.current_slot, field_name)
}

func (tbl_s *TableScan) SetInt(field_name string, val int) {
	tbl_s.e_rm_interface.SetInt(tbl_s.current_slot, field_name, val)
}

func (tbl_s *TableScan) GetString(field_name string) string {
	return tbl_s.e_rm_interface.GetString(tbl_s.current_slot, field_name)
}

func (tbl_s *TableScan) SetString(field_name string, val string) {
	tbl_s.e_rm_interface.SetString(tbl_s.current_slot, field_name, val)
}

func (tbl_s *TableScan) GetVal(field_name string) *Constant {
	if tbl_s.layout.Schema().Type(field_name) == e_rm.INTEGER {
		ival := tbl_s.GetInt(field_name)
		return NewConstantWithInt(&ival)
	}

	sval := tbl_s.GetString(field_name)
	return NewConstantWithString(&sval)
}

func (tbl_s *TableScan) SetVal(field_name string, val *Constant) {
	if tbl_s.layout.Schema().Type(field_name) == e_rm.INTEGER {
		tbl_s.SetInt(field_name, val.AsInt())
	} else {
		tbl_s.SetString(field_name, val.AsString())
	}
}

func (tbl_s *TableScan) MoveToNewBlock() {
	tbl_s.Close()
	blk, err := tbl_s.ts.Append(tbl_s.file_name)
	if err != nil {
		panic(err)
	}

	tbl_s.e_rm_interface = e_rm.NewEntryRecordPage(tbl_s.ts, blk, tbl_s.layout)
	tbl_s.e_rm_interface.Format()
	tbl_s.current_slot = -1
}

func (tbl_s *TableScan) MoveToBlock(blk_num int) {
	tbl_s.Close()
	blk := fm.NewBlockID(tbl_s.file_name, uint64(blk_num))
	tbl_s.e_rm_interface = e_rm.NewEntryRecordPage(tbl_s.ts, blk, tbl_s.layout)
	tbl_s.current_slot = -1
}

func (tbl_s *TableScan) AtLastBlock() bool {
	size, err := tbl_s.ts.File_block_size(tbl_s.file_name)
	if err != nil {
		panic(err)
	}

	return tbl_s.e_rm_interface.Block().Number() == size-1
}

func (tbl_s *TableScan) MoveToRid(r *e_rm.RID) {
	tbl_s.Close()
	blk := fm.NewBlockID(tbl_s.file_name, uint64(r.BlockNumber()))
	tbl_s.e_rm_interface = e_rm.NewEntryRecordPage(tbl_s.ts, blk, tbl_s.layout)
	tbl_s.current_slot = r.Slot()
}

func (tbl_s *TableScan) GetRID() *e_rm.RID {
	return e_rm.NewRID(int(tbl_s.e_rm_interface.Block().Number()), tbl_s.current_slot)
}

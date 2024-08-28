package entry_record_manager

import (
	fm "file_manager"
	"fmt"
	ts "transaction"
)

type SLOT_FLAG int

const (
	EMPTY SLOT_FLAG = iota
	USED
)

type EntryRecordPage struct {
	ts     *ts.Transaction
	blk    *fm.BlockID
	layout LayoutInterface
}

func NewEntryRecordPage(ts *ts.Transaction, blk *fm.BlockID, layout LayoutInterface) *EntryRecordPage {
	e_rp := &EntryRecordPage{
		ts:     ts,
		blk:    blk,
		layout: layout,
	}

	ts.Pin(blk)

	return e_rp
}

func (e_rp *EntryRecordPage) offset(slot int) uint64 {
	return uint64(slot * e_rp.layout.SlotSize())
}

func (e_rp *EntryRecordPage) GetInt(slot int, field_name string) int {
	field_pos := e_rp.offset(slot) + uint64(e_rp.layout.Offset(field_name))
	val, err := e_rp.ts.GetInt(e_rp.blk, field_pos)
	if err == nil {
		return int(val)
	}

	return -1
}

func (e_rp *EntryRecordPage) GetString(slot int, field_name string) string {
	field_pos := e_rp.offset(slot) + uint64(e_rp.layout.Offset(field_name))
	val, _ := e_rp.ts.GetString(e_rp.blk, field_pos)
	return val
}

func (e_rp *EntryRecordPage) SetInt(slot int, field_name string, val int) {
	field_pos := e_rp.offset(slot) + uint64(e_rp.layout.Offset(field_name))
	e_rp.ts.SetInt(e_rp.blk, field_pos, int64(val), true)
}

func (e_rp *EntryRecordPage) SetString(slot int, field_name string, val string) {
	field_pos := e_rp.offset(slot) + uint64(e_rp.layout.Offset(field_name))
	e_rp.ts.SetString(e_rp.blk, field_pos, val, true)
}

func (e_rp *EntryRecordPage) Delete(slot int) {
	e_rp.setFlag(slot, EMPTY)
}

func (e_rp *EntryRecordPage) setFlag(slot int, flag SLOT_FLAG) {
	e_rp.ts.SetInt(e_rp.blk, e_rp.offset(slot), int64(flag), true)
}

func (e_rp *EntryRecordPage) Format() {
	slot := 0
	for e_rp.isValidSlot(slot) {
		e_rp.ts.SetInt(e_rp.blk, e_rp.offset(slot), int64(EMPTY), false)
		sch := e_rp.layout.Schema()
		for _, field_name := range sch.Fields() {
			field_pos := e_rp.offset(slot) + uint64(e_rp.layout.Offset(field_name))
			if sch.Type(field_name) == INTEGER {
				e_rp.ts.SetInt(e_rp.blk, field_pos, 0, false)
			} else {
				e_rp.ts.SetString(e_rp.blk, field_pos, "", false)
			}
			slot += 1
		}
	}
}

func (e_rp *EntryRecordPage) isValidSlot(slot int) bool {
	return e_rp.offset(slot+1) <= e_rp.ts.BlockSize()
}

func (e_rp *EntryRecordPage) NextAfter(slot int) int {
	return e_rp.searchAfter(slot, USED)
}

func (e_rp *EntryRecordPage) InsertAfter(slot int) int {
	new_slot := e_rp.searchAfter(slot, EMPTY)
	if new_slot >= 0 {
		e_rp.setFlag(new_slot, USED)
	}

	return new_slot
}

func (e_rp *EntryRecordPage) searchAfter(slot int, flag SLOT_FLAG) int {
	slot++

	for e_rp.isValidSlot(slot) {
		val, err := e_rp.ts.GetInt(e_rp.blk, e_rp.offset(slot))
		if err != nil {
			//panic(err)
			fmt.Printf("SearchAfter has err %v\n", err)
			return -1
		}

		if SLOT_FLAG(val) == flag {
			return slot
		}
		slot += 1
	}

	return -1
}

func (e_rp *EntryRecordPage) Block() *fm.BlockID {
	return e_rp.blk
}

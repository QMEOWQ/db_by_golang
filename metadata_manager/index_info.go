package metadata_manager

import (
	e_rm "entry_record_manager"
	ts "transaction"
)

type IndexInfo struct {
	idx_name   string
	fld_name   string
	tbl_schema *e_rm.Schema
	ts         *ts.Transaction
	idx_layout *e_rm.Layout
	si         *StatInfo
}

func NewIndexInfo(idx_name string, fld_name string, tbl_schema *e_rm.Schema, ts *ts.Transaction, si *StatInfo) *IndexInfo {
	idx_info := &IndexInfo{
		idx_name:   idx_name,
		fld_name:   fld_name,
		tbl_schema: tbl_schema,
		ts:         ts,
		idx_layout: nil,
		si:         si,
	}

	idx_info.idx_layout = idx_info.CreateIndexLayout()

	return idx_info
}

func (idx_info *IndexInfo) Open() Index {
	//构建不同的哈希对象
	return NewHashIndex(idx_info.ts, idx_info.idx_name, idx_info.idx_layout)
}

func (idx_info *IndexInfo) BlockAccessed() int {
	//rpb : record per block
	rpb := int(idx_info.ts.BlockSize()) / idx_info.idx_layout.SlotSize()
	num_blocks := idx_info.si.RecordsOutput() / rpb
	return HashIndexSearchCost(num_blocks, rpb)
}

func (idx_info *IndexInfo) RecordsOutut() int {
	return idx_info.si.RecordsOutput() / idx_info.si.DistinctVals(idx_info.fld_name)
}

func (idx_info *IndexInfo) DistinctVals(field_name string) int {
	if idx_info.fld_name == field_name {
		return 1
	}

	return idx_info.si.DistinctVals(field_name)
}

func (idx_info *IndexInfo) CreateIndexLayout() *e_rm.Layout {
	sch := e_rm.NewSchema()
	sch.AddIntField("block")
	sch.AddIntField("id")

	if idx_info.tbl_schema.Type(idx_info.fld_name) == e_rm.INTEGER {
		sch.AddIntField("data_val")
	} else {
		fld_len := idx_info.tbl_schema.Length(idx_info.fld_name)
		sch.AddStringField("data_val", fld_len)
	}

	return e_rm.NewLayoutWithSchema(sch)
}

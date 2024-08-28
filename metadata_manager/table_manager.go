package metadata_manager

import (
	e_rm "entry_record_manager"
	ts "transaction"
)

const (
	MAX_NAME = 16 //表名最长16B
)

type TableManager struct {
	tcatLayout *e_rm.Layout //table
	fcatLayout *e_rm.Layout //field
}

func NewTableManager(isNew bool, ts *ts.Transaction) *TableManager {
	table_manager := &TableManager{}
	tcatSchema := e_rm.NewSchema()

	//创建两个表专门用于存储新建数据库表的元数据
	tcatSchema.AddStringField("tbl_name", MAX_NAME)
	tcatSchema.AddIntField("slot_size")
	table_manager.tcatLayout = e_rm.NewLayoutWithSchema(tcatSchema)

	fcatSchema := e_rm.NewSchema()
	fcatSchema.AddStringField("tbl_name", MAX_NAME)
	fcatSchema.AddStringField("fld_name", MAX_NAME)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("len")
	fcatSchema.AddIntField("offset")
	table_manager.fcatLayout = e_rm.NewLayoutWithSchema(fcatSchema)

	if isNew {
		//如果当前数据表是第一次创建，那么为这个表创建两个元数据表
		table_manager.CreateTable("tblcat", tcatSchema, ts)
		table_manager.CreateTable("fldcat", fcatSchema, ts)
	}

	return table_manager
}

func (tm *TableManager) CreateTable(tbl_name string, sch *e_rm.Schema, ts *ts.Transaction) {
	//在创建数据表前先创建tblcat, fldcat两个元数据表
	layout := e_rm.NewLayoutWithSchema(sch)
	tcat := e_rm.NewTableScan(ts, "tblcat", tm.tcatLayout)

	tcat.Insert()
	tcat.SetString("tbl_name", tbl_name)
	tcat.SetInt("slot_size", layout.SlotSize())
	tcat.Close()

	fcat := e_rm.NewTableScan(ts, "fldcat", tm.fcatLayout)
	for _, fld_name := range sch.Fields() {
		fcat.Insert()
		fcat.SetString("tbl_name", tbl_name)
		fcat.SetString("fld_name", fld_name)
		fcat.SetInt("type", int(sch.Type(fld_name)))
		fcat.SetInt("len", int(sch.Length(fld_name)))
		fcat.SetInt("offset", layout.Offset(fld_name))
	}
	fcat.Close()
}

func (tm *TableManager) GetLayout(tbl_name string, ts *ts.Transaction) *e_rm.Layout {
	//获取表的layout结构
	size := -1
	tcat := e_rm.NewTableScan(ts, "tblcat", tm.tcatLayout)
	for tcat.Next() {
		//找到表名对应的元数据表
		if tcat.GetString("tbl_name") == tbl_name {
			size = tcat.GetInt("slot_size")
			break
		}
	}
	tcat.Close()

	sch := e_rm.NewSchema()
	offsets := make(map[string]int)
	fcat := e_rm.NewTableScan(ts, "fldcat", tm.fcatLayout)
	for fcat.Next() {
		if fcat.GetString("tbl_name") == tbl_name {
			fld_name := fcat.GetString("fld_name")
			fld_type := fcat.GetInt("type")
			fld_len := fcat.GetInt("len")
			fld_offset := fcat.GetInt("offset")
			offsets[fld_name] = fld_offset
			sch.AddField(fld_name, e_rm.FIELD_TYPE(fld_type), fld_len)
		}
	}
	fcat.Close()

	return e_rm.NewLayout(sch, offsets, size)
}

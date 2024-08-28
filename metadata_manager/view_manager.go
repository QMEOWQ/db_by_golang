package metadata_manager

import (
	e_rm "entry_record_manager"
	ts "transaction"
)

const (
	MAX_VIEWDEF = 1000
)

type ViewManager struct {
	tblManager *TableManager
}

func NewViewManager(isNew bool, tbl_manager *TableManager, ts *ts.Transaction) *ViewManager {
	view_manager := &ViewManager{
		tblManager: tbl_manager,
	}

	if isNew {
		sch := e_rm.NewSchema()
		sch.AddStringField("view_name", MAX_NAME)
		sch.AddStringField("view_def", MAX_VIEWDEF)
		//view_manager.tblManager.CreateTable("viewcat", sch, ts)
		tbl_manager.CreateTable("viewcat", sch, ts)
	}

	return view_manager
}

func (vm *ViewManager) CreateView(view_name string, view_def string, ts *ts.Transaction) {
	//每创建一个视图对象，就在viewcat表中插入一条对该视图对象元数据的记录
	layout := vm.tblManager.GetLayout("viewcat", ts)
	tbs := e_rm.NewTableScan(ts, "viewcat", layout)
	tbs.Insert()
	tbs.SetString("view_name", view_name)
	tbs.SetString("view_def", view_def)
	tbs.Close()
}

func (vm *ViewManager) GetViewDef(view_name string, ts *ts.Transaction) string {
	res := ""
	layout := vm.tblManager.GetLayout("viewcat", ts)
	tbs := e_rm.NewTableScan(ts, "viewcat", layout)
	for tbs.Next() {
		if tbs.GetString("view_name") == view_name {
			res = tbs.GetString("view_def")
			break
		}
	}

	tbs.Close()

	return res
}

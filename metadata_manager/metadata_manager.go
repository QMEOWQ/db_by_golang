package metadata_manager

import (
	e_rm "entry_record_manager"
	ts "transaction"
)

type MetaDataManager struct {
	tblManager  *TableManager
	viewManager *ViewManager
	statManager *StatManager
}

func NewMetaDataManager(isNew bool, ts *ts.Transaction) *MetaDataManager {
	metadata_manager := &MetaDataManager{
		tblManager: NewTableManager(isNew, ts),
	}

	metadata_manager.viewManager = NewViewManager(isNew, metadata_manager.tblManager, ts)
	metadata_manager.statManager = NewStatManager(metadata_manager.tblManager, ts)

	return metadata_manager
}

func (mdm *MetaDataManager) CreateTable(tbl_name string, sch *e_rm.Schema, ts *ts.Transaction) {
	mdm.tblManager.CreateTable(tbl_name, sch, ts)
}

func (mdm *MetaDataManager) CreateView(view_name string, view_def string, ts *ts.Transaction) {
	mdm.viewManager.CreateView(view_name, view_def, ts)
}

func (mdm *MetaDataManager) GetLayout(tbl_name string, ts *ts.Transaction) *e_rm.Layout {
	return mdm.tblManager.GetLayout(tbl_name, ts)
}

func (mdm *MetaDataManager) GetViewDef(view_name string, ts *ts.Transaction) string {
	return mdm.viewManager.GetViewDef(view_name, ts)
}

func (mdm *MetaDataManager) GetStatInfo(tbl_name string, layout *e_rm.Layout, ts *ts.Transaction) *StatInfo {
	return mdm.statManager.GetStatInfo(tbl_name, layout, ts)
}

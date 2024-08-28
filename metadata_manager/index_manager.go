package metadata_manager

import (
	e_rm "entry_record_manager"
	"query"
	ts "transaction"
)

type IndexManager struct {
	layout      *e_rm.Layout
	tblManager  *TableManager
	statManager *StatManager
}

func NewIndexManager(isNew bool, tbl_manager *TableManager, stat_manager *StatManager, ts *ts.Transaction) *IndexManager {
	if isNew {
		//索引名，对应的表名，被索引的字段名
		sch := e_rm.NewSchema()
		sch.AddStringField("index_name", MAX_NAME)
		sch.AddStringField("tbl_name", MAX_NAME)
		sch.AddStringField("field_name", MAX_NAME)
		tbl_manager.CreateTable("idx_cat", sch, ts)
	}

	idx_manager := &IndexManager{
		layout:      tbl_manager.GetLayout("idx_cat", ts),
		tblManager:  tbl_manager,
		statManager: stat_manager,
	}

	return idx_manager
}

func (im *IndexManager) CreateIndex(idx_name string, tbl_name string, fld_name string, ts *ts.Transaction) {
	tbs := query.NewTableScan(ts, "idx_cat", im.layout)
	tbs.BeforeFirst()
	tbs.Insert()
	tbs.SetString("index_name", idx_name)
	tbs.SetString("tbl_name", tbl_name)
	tbs.SetString("field_name", fld_name)
	tbs.Close()
}

func (im *IndexManager) GetIndexInfo(tbl_name string, ts *ts.Transaction) map[string]*IndexInfo {
	res := make(map[string]*IndexInfo)
	tbs := query.NewTableScan(ts, "idx_cat", im.layout)

	tbs.BeforeFirst()
	for tbs.Next() {
		if tbs.GetString("tbl_name") == tbl_name {
			idx_name := tbs.GetString("idx_name")
			fld_name := tbs.GetString("field_name")
			tbl_layout := im.tblManager.GetLayout(tbl_name, ts)
			tbl_stat_info := im.statManager.GetStatInfo(tbl_name, tbl_layout, ts)
			sch, ok := (tbl_layout.Schema()).(*e_rm.Schema) //类型断言
			if ok != true {
				panic("convert schema interface error!")
			}
			idx_info := NewIndexInfo(idx_name, fld_name, sch, ts, tbl_stat_info)
			res[idx_name] = idx_info
		}
	}

	tbs.Close()

	return res
}

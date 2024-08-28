package metadata_manager

import (
	e_rm "entry_record_manager"
	"sync"
	ts "transaction"
)

const (
	REFRESH_STAT_INFO_COUNT = 1000 //数据库表发生变化1000次后刷新统计信息
)

type StatInfo struct {
	numBlocks int
	numRecs   int
}

func newStatInfo(numBlocks int, numRecs int) *StatInfo {
	return &StatInfo{
		numBlocks: numBlocks,
		numRecs:   numRecs,
	}
}

func (si *StatInfo) BlockAccessed() int {
	return si.numBlocks
}

func (si *StatInfo) RecordsOutput() int {
	return si.numRecs
}

func (si *StatInfo) DistinctVals(fld_name string) int {
	//返回不重复的字段数, 默认值设为1/3
	return 1 + (si.numRecs / 3)
}

type StatManager struct {
	tblManager *TableManager
	tblStats   map[string]*StatInfo
	numCalls   int
	lock       sync.Mutex
}

func NewStatManager(tblManager *TableManager, ts *ts.Transaction) *StatManager {
	statManager := &StatManager{
		tblManager: tblManager,
		//tblStats: make(map[string]*StatInfo),
		numCalls: 0,
	}

	statManager.refreshStats(ts)

	return statManager
}

func (sm *StatManager) GetStatInfo(tbl_name string, layout *e_rm.Layout, ts *ts.Transaction) *StatInfo {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	sm.numCalls++
	if sm.numCalls > REFRESH_STAT_INFO_COUNT {
		sm.refreshStats(ts)
	}

	si := sm.tblStats[tbl_name]
	if si == nil {
		si = sm.calcTableStats(tbl_name, layout, ts)
		sm.tblStats[tbl_name] = si
	}

	return si
}

func (sm *StatManager) refreshStats(ts *ts.Transaction) {
	sm.tblStats = make(map[string]*StatInfo)
	sm.numCalls = 0

	tcatLayout := sm.tblManager.GetLayout("tblcat", ts)
	tcat := e_rm.NewTableScan(ts, "tblcat", tcatLayout)
	for tcat.Next() {
		tbl_name := tcat.GetString("tbl_name")
		layout := sm.tblManager.GetLayout(tbl_name, ts)
		si := sm.calcTableStats(tbl_name, layout, ts)
		sm.tblStats[tbl_name] = si
	}

	tcat.Close()
}

func (sm *StatManager) calcTableStats(tbl_name string, layout *e_rm.Layout, ts *ts.Transaction) *StatInfo {
	num_blocks := 0
	num_recs := 0

	tbs := e_rm.NewTableScan(ts, tbl_name, layout)
	for tbs.Next() {
		num_blocks = tbs.GetRID().BlockNumber() + 1
		num_recs++
	}
	tbs.Close()

	return newStatInfo(num_blocks, num_recs)
}

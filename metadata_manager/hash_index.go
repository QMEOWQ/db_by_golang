package metadata_manager

import (
	e_rm "entry_record_manager"
	"fmt"
	"query"
	ts "transaction"
)

const (
	NUM_BUCKETS = 1000 //将所有表项放入1000个桶中
)

type HashIndex struct {
	ts         *ts.Transaction
	idx_name   string
	layout     e_rm.Layout
	search_key *query.Constant
	tbs        *query.TableScan
}

func NewHashIndex(ts *ts.Transaction, idx_name string, layout *e_rm.Layout) *HashIndex {
	return &HashIndex{
		ts:       ts,
		idx_name: idx_name,
		layout:   *layout,
		tbs:      nil,
	}
}

func (hi *HashIndex) FirstQualify(search_key *query.Constant) {
	hi.Close()

	hi.search_key = search_key
	bucket := search_key.HashCode() % NUM_BUCKETS

	tbl_name := fmt.Sprintf("%s#%d", hi.idx_name, bucket)
	hi.tbs = query.NewTableScan(hi.ts, tbl_name, &hi.layout)
}

func (hi *HashIndex) Next() bool {
	for hi.tbs.Next() {
		if hi.tbs.GetVal("data_val").Equals(hi.search_key) {
			return true
		}
	}

	return false
}

func (hi *HashIndex) GetDataRID() *e_rm.RID {
	blk_num := hi.tbs.GetInt("block")
	id := hi.tbs.GetInt("id")
	return e_rm.NewRID(blk_num, id)
}

func (hi *HashIndex) Insert(val *query.Constant, rid *e_rm.RID) {
	hi.FirstQualify(val)
	hi.tbs.Insert()
	hi.tbs.SetInt("block", rid.BlockNumber())
	hi.tbs.SetInt("id", rid.Slot())
	hi.tbs.SetVal("data_val", val)
}

func (hi *HashIndex) Delete(val *query.Constant, rid *e_rm.RID) {
	hi.FirstQualify(val)
	for hi.Next() {
		if hi.GetDataRID().Equals(rid) {
			hi.tbs.Delete()
			return
		}
	}
}

func (hi *HashIndex) Close() {
	if hi.tbs != nil {
		hi.tbs.Close()
	}
}

func HashIndexSearchCost(num_blocks int, rpb int) int {
	return num_blocks / NUM_BUCKETS
}

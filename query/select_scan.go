package query

import (
	e_rm "entry_record_manager"
)

type SelectScan struct {
	updateScan UpdateScan
	pred       *Predicate
}

func NewSelectScan(us UpdateScan, pred *Predicate) *SelectScan {
	return &SelectScan{
		updateScan: us,
		pred:       pred,
	}
}

func (s *SelectScan) FirstQualify() {
	s.updateScan.GetScan().FirstQualify()
}

func (s *SelectScan) Next() bool {
	for s.updateScan.GetScan().Next() {
		if s.pred.IsSatisfied(s.updateScan.GetScan()) {
			return true
		}
	}

	return false
}

func (s *SelectScan) GetInt(fld_name string) int {
	return s.updateScan.GetScan().GetInt(fld_name)
}

func (s *SelectScan) GetString(fld_name string) string {
	return s.updateScan.GetScan().GetString(fld_name)
}

func (s *SelectScan) GetVal(fld_name string) *Constant {
	return s.updateScan.GetScan().GetVal(fld_name)
}

func (s *SelectScan) SetInt(fld_name string, val int) {
	s.updateScan.SetInt(fld_name, val)
}

func (s *SelectScan) SetString(fld_name string, val string) {
	s.updateScan.SetString(fld_name, val)
}

func (s *SelectScan) SetVal(fld_name string, val *Constant) {
	s.updateScan.SetVal(fld_name, val)
}

func (s *SelectScan) HasField(fld_name string) bool {
	return s.updateScan.GetScan().HasField(fld_name)
}

func (s *SelectScan) Insert() {
	s.updateScan.Insert()
}

func (s *SelectScan) Delete() {
	s.updateScan.Delete()
}

func (s *SelectScan) Close() {
	s.updateScan.GetScan().Close()
}

func (s *SelectScan) GetRID() *e_rm.RID {
	return s.updateScan.GetRID()
}

func (s *SelectScan) MoveToRID(rid *e_rm.RID) {
	s.updateScan.MoveToRID(rid)
}

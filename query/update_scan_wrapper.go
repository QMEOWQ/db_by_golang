package query

import (
	e_rm "entry_record_manager"
)

type UpdateScanWrapper struct {
	scan Scan
}

func NewUpdateScanWrapper(scan Scan) *UpdateScanWrapper {
	return &UpdateScanWrapper{
		scan: scan,
	}
}

func (usw *UpdateScanWrapper) GetScan() Scan {
	return usw.scan
}

func (u *UpdateScanWrapper) SetInt(fldName string, val int) {
	//DO NOTHING
}

func (u *UpdateScanWrapper) SetString(fldName string, val string) {
	//DO NOTHING
}

func (u *UpdateScanWrapper) SetVal(fldName string, val *Constant) {
	//DO NOTHING
}

func (u *UpdateScanWrapper) Insert() {
	//DO NOTHING
}

func (u *UpdateScanWrapper) Delete() {
	//DO NOTHING
}

func (u *UpdateScanWrapper) GetRID() *e_rm.RID {
	return nil
}

func (u *UpdateScanWrapper) MoveToRID(rid *e_rm.RID) {
	// DO NOTHING
}

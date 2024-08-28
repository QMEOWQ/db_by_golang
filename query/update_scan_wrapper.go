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

func (usw *UpdateScanWrapper) SetInt(fldName string, val int) {
	//DO NOTHING
}

func (usw *UpdateScanWrapper) SetString(fldName string, val string) {
	//DO NOTHING
}

func (usw *UpdateScanWrapper) SetVal(fldName string, val *Constant) {
	//DO NOTHING
}

func (usw *UpdateScanWrapper) Insert() {
	//DO NOTHING
}

func (usw *UpdateScanWrapper) Delete() {
	//DO NOTHING
}

func (usw *UpdateScanWrapper) GetRID() *e_rm.RID {
	return nil
}

func (usw *UpdateScanWrapper) MoveToRID(rid *e_rm.RID) {
	// DO NOTHING
}

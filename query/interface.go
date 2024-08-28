package query

import (
	e_rm "entry_record_manager"
)

type Scan interface {
	FirstQualify()
	Next() bool
	GetInt(fld_name string) int
	GetString(fld_name string) string
	GetVal(fld_name string) *Constant
	HasField(fld_name string) bool
	Close()
}

type UpdateScan interface {
	GetScan() Scan
	SetInt(fld_name string, val int)
	SetString(fld_name string, val string)
	SetVal(fld_name string, val *Constant)
	Insert()
	Delete()
	GetRID() *e_rm.RID
	MoveToRID(rid *e_rm.RID)
}

type Plan interface {
	Open() interface{}
	BlocksAccessed() int
	RecordsOutput() int
	DistinctVals(fld_name string) int
	Schema() e_rm.SchemaInterface
}

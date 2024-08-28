package transaction

import (
	fm "file_manager"
	lm "log_manager"
	"math"
)

type CheckPointRecord struct {
	//ts_num uint64
}

func NewCheckPointRecord() *CheckPointRecord {
	return &CheckPointRecord{}
}

func (rbr *CheckPointRecord) Op() RECORD_TYPE {
	return CHECKPOINT
}

func (rbr *CheckPointRecord) TsNumber() uint64 {
	//该操作无对应的交易号
	return math.MaxUint64
}

func (rbr *CheckPointRecord) Undo(_ TransactionInterface) {
	//nothing to do here
}

func (rbr *CheckPointRecord) ToString() string {
	return "<CHECKPOINT>"
}

func WriteCheckPointRecordLog(log_manager *lm.LogManager) (uint64, error) {
	rec := make([]byte, UINT64_LENGTH) // 1 metadatas
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(CHECKPOINT))

	return log_manager.Append(rec)
}

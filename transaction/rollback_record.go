package transaction

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type RollBackRecord struct {
	ts_num uint64
}

func NewRollBackRecord(p *fm.Page) *RollBackRecord {
	return &RollBackRecord{
		ts_num: p.GetInt(UINT64_LENGTH),
	}
}

func (rbr *RollBackRecord) Op() RECORD_TYPE {
	return ROLLBACK
}

func (rbr *RollBackRecord) TsNumber() uint64 {
	return rbr.ts_num
}

func (rbr *RollBackRecord) Undo(_ TransactionInterface) {
	//nothing to do here
}

func (rbr *RollBackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", rbr.ts_num)
}

func WriteRollBackRecordLog(log_manager *lm.LogManager, ts_num uint64) (uint64, error) {
	rec := make([]byte, 2*UINT64_LENGTH) // commit ts_num 2 metadatas
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(ROLLBACK))
	p.SetInt(UINT64_LENGTH, ts_num)

	return log_manager.Append(rec)
}

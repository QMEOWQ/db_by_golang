package transaction

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type CommitRecord struct {
	ts_num uint64
}

func NewCommitRecord(p *fm.Page) *CommitRecord {
	return &CommitRecord{
		ts_num: p.GetInt(UINT64_LENGTH),
	}
}

func (cr *CommitRecord) Op() RECORD_TYPE {
	return COMMIT
}

func (cr *CommitRecord) TsNumber() uint64 {
	return cr.ts_num
}

func (cr *CommitRecord) Undo(_ TransactionInterface) {
	//nothing to do here
}

func (cr *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", cr.ts_num)
}

func WriteCommitRecordLog(log_manager *lm.LogManager, ts_num uint64) (uint64, error) {
	rec := make([]byte, 2*UINT64_LENGTH) // commit ts_num 2 metadatas
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(COMMIT))
	p.SetInt(UINT64_LENGTH, ts_num)

	return log_manager.Append(rec)
}

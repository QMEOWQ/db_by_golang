package transaction

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type StartRecord struct {
	ts_num      uint64
	log_manager *lm.LogManager
}

func NewStartRecord(p *fm.Page, log_manager *lm.LogManager) *StartRecord {
	//开头的8B对应日志的类型，接下来8B对应交易号
	ts_num := p.GetInt(UINT64_LENGTH)
	return &StartRecord{
		ts_num:      ts_num,
		log_manager: log_manager,
	}
}

func (sr *StartRecord) Op() RECORD_TYPE {
	return START
}

func (sr *StartRecord) TsNumber() uint64 {
	return sr.ts_num
}

func (sr *StartRecord) Undo(_ TransactionInterface) {
	//start record不能回滚，所以什么也不做
}

func (sr *StartRecord) ToString() string {
	str := fmt.Sprintf("<START %d>", sr.ts_num)
	return str
}

func (sr *StartRecord) WriteToLog() (uint64, error) {
	record := make([]byte, 2*UINT64_LENGTH)

	p := fm.NewPageByBytes(record)
	p.SetInt(uint64(0), uint64(START))
	p.SetInt(UINT64_LENGTH, sr.ts_num)

	return sr.log_manager.Append(record)
}

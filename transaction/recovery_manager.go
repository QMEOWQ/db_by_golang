package transaction

import (
	bm "buffer_manager"
	fm "file_manager"
	lm "log_manager"
)

type RecoveryManager struct {
	log_manager    *lm.LogManager
	buffer_manager *bm.BufferManager
	ts             *Transaction
	ts_num         int32
}

func NewRecoveryManager(ts *Transaction, ts_num int32, log_manager *lm.LogManager, buffer_manager *bm.BufferManager) *RecoveryManager {
	recovery_manager := &RecoveryManager{
		ts:             ts,
		ts_num:         ts_num,
		log_manager:    log_manager,
		buffer_manager: buffer_manager,
	}

	p := fm.NewPageBySize(32)
	p.SetInt(0, uint64(START))
	p.SetInt(8, uint64(ts_num))
	start_record := NewStartRecord(p, log_manager)
	start_record.WriteToLog()

	return recovery_manager
}

func (rm *RecoveryManager) Commit() error {
	rm.buffer_manager.FlushAll(rm.ts_num)
	lsn, err := WriteCommitRecordLog(rm.log_manager, uint64(rm.ts_num))
	if err != nil {
		return err
	}

	rm.log_manager.FlushByLSN(lsn)

	return nil
}

func (rm *RecoveryManager) Rollback() error {
	rm.doRollback()

	rm.buffer_manager.FlushAll(rm.ts_num)
	lsn, err := WriteRollBackRecordLog(rm.log_manager, uint64(rm.ts_num))
	if err != nil {
		return err
	}

	rm.log_manager.FlushByLSN(lsn)

	return nil
}

func (rm *RecoveryManager) doRollback() {
	iter := rm.log_manager.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		log_rec := rm.CreateLogRecord(rec)
		if log_rec.TsNumber() == uint64(rm.ts_num) {
			if log_rec.Op() == START {
				return
			}

			log_rec.Undo(rm.ts)
		}

	}
}

func (rm *RecoveryManager) Recover() error {
	rm.doRecover()

	rm.buffer_manager.FlushAll(rm.ts_num)
	lsn, err := WriteCheckPointRecordLog(rm.log_manager)
	if err != nil {
		return err
	}

	rm.log_manager.FlushByLSN(lsn)

	return nil
}

func (rm *RecoveryManager) doRecover() {
	finished_ts := make(map[uint64]bool)
	iter := rm.log_manager.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		log_rec := rm.CreateLogRecord(rec)
		if log_rec.Op() == CHECKPOINT {
			return
		}

		if log_rec.Op() == COMMIT || log_rec.Op() == ROLLBACK {
			finished_ts[log_rec.TsNumber()] = true
		}

		existed, ok := finished_ts[log_rec.TsNumber()]
		if !existed && !ok {
			log_rec.Undo(rm.ts)
		}

	}
}

func (rm *RecoveryManager) SetInt(buffer *bm.Buffer, offset uint64, new_val int64) (uint64, error) {
	old_val := buffer.Contents().GetInt(offset)
	blk := buffer.Block()
	buffer.Contents().SetInt(offset, uint64(new_val))
	return WriteSetIntLog(rm.log_manager, uint64(rm.ts_num), blk, offset, old_val)
}

func (rm *RecoveryManager) SetString(buffer *bm.Buffer, offset uint64, new_val string) (uint64, error) {
	old_val := buffer.Contents().GetString(offset)
	blk := buffer.Block()
	buffer.Contents().SetString(offset, new_val)
	return WriteSetStringLog(rm.log_manager, uint64(rm.ts_num), blk, offset, old_val)
}

func (rm *RecoveryManager) CreateLogRecord(bytes []byte) LogRecordInterface {
	p := fm.NewPageByBytes(bytes)

	switch RECORD_TYPE(p.GetInt(0)) {
	case CHECKPOINT:
		return NewCheckPointRecord()
	case START:
		return NewStartRecord(p, rm.log_manager)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollBackRecord(p)
	case SETINT:
		return NewSetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	default:
		panic("unknown log interface")
	}
}

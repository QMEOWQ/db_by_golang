package transaction

import (
	bm "buffer_manager"
	"errors"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"sync"
)

var ts_num_mutex sync.Mutex
var next_ts_num = int32(0)

func nextTsNum() int32 {
	ts_num_mutex.Lock()
	defer ts_num_mutex.Unlock()

	next_ts_num += 1

	return next_ts_num
}

type Transaction struct {
	concurr_manager  *ConCurrencyManager
	recovery_manager *RecoveryManager
	file_manager     *fm.FileManager
	log_manager      *lm.LogManager
	buffer_manager   *bm.BufferManager
	buffers          *BufferList
	ts_num           int32
}

func NewTransaction(file_manager *fm.FileManager, log_manager *lm.LogManager, buffer_manager *bm.BufferManager) *Transaction {
	ts_num := nextTsNum()

	ts := &Transaction{
		file_manager:   file_manager,
		log_manager:    log_manager,
		buffer_manager: buffer_manager,
		buffers:        NewBufferList(buffer_manager),
		ts_num:         ts_num,
	}

	//创建并发管理器
	ts.concurr_manager = NewConCurrencyManager()

	//创建恢复管理器
	ts.recovery_manager = NewRecoveryManager(ts, ts_num, log_manager, buffer_manager)

	return ts
}

func (ts *Transaction) Commit() {
	ts.concurr_manager.Release()
	ts.recovery_manager.Commit()

	str := fmt.Sprintf("transaction %d committed", ts.ts_num)
	fmt.Println(str)

	//释放并发管理器
	ts.buffers.UnPinAll()
}

func (ts *Transaction) RollBack() {
	ts.recovery_manager.Rollback()
	ts.concurr_manager.Release()

	str := fmt.Sprintf("transaction %d rolled back", ts.ts_num)
	fmt.Println(str)

	ts.buffers.UnPinAll()
}

// 将给定交易的数据修改全部写入磁盘
func (ts *Transaction) Recover() {
	//系统启动时会在所有交易执行前先执行该操作
	ts.buffer_manager.FlushAll(ts.ts_num)
	ts.recovery_manager.Recover()
}

func (ts *Transaction) Pin(blk *fm.BlockID) {
	ts.buffers.Pin(blk)
}

func (ts *Transaction) UnPin(blk *fm.BlockID) {
	ts.buffers.UnPin(blk)
}

func (ts *Transaction) buffer_not_exist(blk *fm.BlockID) error {
	err_str := fmt.Sprintf("No buffer found for given blk : %d with file_name : %s\n", blk.Number(), blk.FileName())
	err := errors.New(err_str)
	return err
}

func (ts *Transaction) GetInt(blk *fm.BlockID, offset uint64) (int64, error) {
	//并发管理器加s锁
	err := ts.concurr_manager.SLock(blk)
	if err != nil {
		return -1, err
	}

	buff := ts.buffers.get_buffer(blk)
	if buff == nil {
		return -1, ts.buffer_not_exist(blk)
	}

	return int64(buff.Contents().GetInt(offset)), nil
}

func (ts *Transaction) GetString(blk *fm.BlockID, offset uint64) (string, error) {
	//并发管理器加s锁
	err := ts.concurr_manager.SLock(blk)
	if err != nil {
		return "", err
	}

	buff := ts.buffers.get_buffer(blk)
	if buff == nil {
		return "", ts.buffer_not_exist(blk)
	}

	return buff.Contents().GetString(offset), nil
}

func (ts *Transaction) SetInt(blk *fm.BlockID, offset uint64, val int64, okToLog bool) error {
	//并发管理器加x锁
	err := ts.concurr_manager.XLock(blk)
	if err != nil {
		return err
	}

	//var err error

	buff := ts.buffers.get_buffer(blk)
	if buff == nil {
		return ts.buffer_not_exist(blk)
	}

	var lsn uint64
	if okToLog {
		lsn, err = ts.recovery_manager.SetInt(buff, offset, val)
		if err != nil {
			return err
		}
	}

	p := buff.Contents()
	p.SetInt(offset, uint64(val))
	buff.SetModified(ts.ts_num, lsn)
	return nil
}

func (ts *Transaction) SetString(blk *fm.BlockID, offset uint64, val string, okToLog bool) error {
	//并发管理器加x锁
	err := ts.concurr_manager.XLock(blk)
	if err != nil {
		return err
	}

	//var err error

	buff := ts.buffers.get_buffer(blk)
	if buff == nil {
		return ts.buffer_not_exist(blk)
	}

	var lsn uint64
	if okToLog {
		lsn, err = ts.recovery_manager.SetString(buff, offset, val)
		if err != nil {
			return err
		}
	}

	p := buff.Contents()
	p.SetString(offset, val)
	buff.SetModified(ts.ts_num, lsn)
	return nil
}

func (ts *Transaction) File_block_size(file_name string) (uint64, error) {
	//并发管理器加锁
	dummy_blk := fm.NewBlockID(file_name, uint64(END_OF_FILE))
	err := ts.concurr_manager.SLock(dummy_blk)
	if err != nil {
		return 0, err
	}

	s, _ := ts.file_manager.File_block_size(file_name)
	return s, nil
}

func (ts *Transaction) Append(file_name string) (*fm.BlockID, error) {
	//并发管理器加锁
	dummy_blk := fm.NewBlockID(file_name, uint64(END_OF_FILE))
	err := ts.concurr_manager.XLock(dummy_blk)
	if err != nil {
		return nil, err
	}

	blk, err := ts.file_manager.Append(file_name)
	if err != nil {
		return nil, err
	}

	return &blk, nil
}

func (ts *Transaction) BlockSize() uint64 {
	return ts.file_manager.BlockSize()
}

func (ts *Transaction) AvailableBuffers() uint64 {
	return uint64(ts.buffer_manager.Available())
}

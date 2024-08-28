package transaction

import (
	fm "file_manager"
)

type TransactionInterface interface {
	Commit()
	RollBack()
	Recover()
	Pin(blk *fm.BlockID)
	UnPin(blk *fm.BlockID)
	GetInt(blk *fm.BlockID, offset uint64) (int64, error)
	GetString(blk *fm.BlockID, offset uint64) (string, error)
	SetInt(blk *fm.BlockID, offset uint64, value int64, okToLog bool) error
	SetString(blk *fm.BlockID, offset uint64, value string, okToLog bool) error
	AvailableBuffers() uint64
	File_block_size(file_name string) (uint64, error)
	Append(file_name string) (*fm.BlockID, error)
	BlockSize() uint64
}

type RECORD_TYPE uint64

const (
	CHECKPOINT RECORD_TYPE = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

const (
	UINT64_LENGTH = 8
	END_OF_FILE   = -1
)

type LogRecordInterface interface {
	Op() RECORD_TYPE
	TsNumber() uint64
	Undo(ts TransactionInterface)
	ToString() string
}

type LockTableInterface interface {
	SLock(blk fm.BlockID)  //共享锁
	XLock(blk fm.BlockID)  //互斥锁
	Unlock(blk fm.BlockID) //解锁
}

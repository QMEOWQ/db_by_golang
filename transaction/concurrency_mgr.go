package transaction

import (
	fm "file_manager"
)

type ConCurrencyManager struct {
	lock_table *LockTable
	lock_map   map[fm.BlockID]string
}

func NewConCurrencyManager() *ConCurrencyManager {
	return &ConCurrencyManager{
		lock_table: GetLockTableInstance(),
		lock_map:   make(map[fm.BlockID]string),
	}
}

func (cm *ConCurrencyManager) SLock(blk *fm.BlockID) error {
	_, ok := cm.lock_map[*blk]
	if !ok {
		err := cm.lock_table.SLock(blk)
		if err != nil {
			return err
		}
		cm.lock_map[*blk] = "S"
	}

	return nil
}

func (cm *ConCurrencyManager) XLock(blk *fm.BlockID) error {
	if !cm.hasXLock(blk) {
		err := cm.lock_table.XLock(blk)
		if err != nil {
			return err
		}
		cm.lock_map[*blk] = "X"
	}

	return nil
}

func (cm *ConCurrencyManager) Release() {
	for key, _ := range cm.lock_map {
		cm.lock_table.Unlock(&key)
	}
}

func (cm *ConCurrencyManager) hasXLock(blk *fm.BlockID) bool {
	lock_type, ok := cm.lock_map[*blk]

	return ok && lock_type == "X"
}
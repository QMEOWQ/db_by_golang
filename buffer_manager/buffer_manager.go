package buffer_manager

import (
	"errors"
	fm "file_manager"
	lm "log_manager"
	"sync"
	"time"
)

const (
	MAX_TIME = 3 //分配页面最多等待的时间计数
)

type BufferManager struct {
	buffer_pool   []*Buffer
	num_available uint32
	mutex         sync.Mutex
}

func NewBufferManager(fm *fm.FileManager, lm *lm.LogManager, num_buffers uint32) *BufferManager {
	buffer_manager := &BufferManager{
		num_available: num_buffers,
	}

	for i := uint32(0); i < num_buffers; i++ {
		buffer := NewBuffer(fm, lm)
		buffer_manager.buffer_pool = append(buffer_manager.buffer_pool, buffer)
	}

	return buffer_manager
}

func (bm *BufferManager) Available() uint32 {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	return bm.num_available
}

func (bm *BufferManager) FlushAll(tsnum int32) {
	//将给定交易的数据修改全部写入磁盘
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	for _, buffer := range bm.buffer_pool {
		if buffer.ModifyingTs() == tsnum {
			buffer.Flush()
		}
	}
}

func (bm *BufferManager) Pin(blk *fm.BlockID) (*Buffer, error) {
	//将给定磁盘文件的区块数据分配给缓存页面
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	start := time.Now()
	buff := bm.tryPin(blk) //尝试分配缓存
	for buff == nil && bm.waitingTooLong(start) == false {
		//如果无法分配到缓存页面，等待一段时间再看
		time.Sleep(MAX_TIME * time.Second)
		buff = bm.tryPin(blk)
		if buff == nil {
			return nil, errors.New("No buffer AVAILABLE, careful for dead lock.")
		}
	}

	return buff, nil
}

func (bm *BufferManager) UnPin(buff *Buffer) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if buff == nil {
		return
	}

	buff.UnPin()
	if !buff.IsPinned() {
		bm.num_available += 1

		//notifyAll: 唤醒所有等待它的线程->并发管理器
	}
}

func (bm *BufferManager) tryPin(blk *fm.BlockID) *Buffer {
	//首先判断给定区块是否已经读取到了某个页面
	buff := bm.findExistingBuffer(blk)
	if buff == nil {
		//查看是否还有可用的缓存页面，有的话将给定磁盘数据写入缓存
		buff = bm.chooseUnpinBuffer()
		if buff == nil {
			return nil
		}
		buff.AssignToBlock(blk)
	}

	if buff.IsPinned() == false {
		bm.num_available -= 1
	}

	buff.Pin()
	return buff
}

func (bm *BufferManager) findExistingBuffer(blk *fm.BlockID) *Buffer {
	for _, buffer := range bm.buffer_pool {
		block := buffer.Block()
		if block != nil && block.Equal(blk) {
			return buffer
		}
	}
	return nil
}

func (bm *BufferManager) chooseUnpinBuffer() *Buffer {
	for _, buffer := range bm.buffer_pool {
		if !buffer.IsPinned() {
			return buffer
		}
	}
	return nil
}

func (bm *BufferManager) waitingTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()
	if elapsed >= MAX_TIME {
		return true
	}

	return false
}

//对被pin的buffer进行管理

package transaction

import (
	bm "buffer_manager"
	fm "file_manager"
)

type BufferList struct {
	buffers    map[fm.BlockID]*bm.Buffer
	buffer_mgr *bm.BufferManager
	pins       []fm.BlockID
}

func NewBufferList(buffer_mgr *bm.BufferManager) *BufferList {
	return &BufferList{
		buffer_mgr: buffer_mgr,
		buffers:    make(map[fm.BlockID]*bm.Buffer),
		pins:       make([]fm.BlockID, 0),
	}
}

func (bl *BufferList) get_buffer(blk *fm.BlockID) *bm.Buffer {
	buffer, _ := bl.buffers[*blk]
	return buffer
}

func (bl *BufferList) Pin(blk *fm.BlockID) error {
	//缓存页被pin, 加入map建立映射方便管理
	buffer, err := bl.buffer_mgr.Pin(blk)
	if err != nil {
		return err
	}

	bl.buffers[*blk] = buffer
	bl.pins = append(bl.pins, *blk)

	return nil
}

func (bl *BufferList) UnPin(blk *fm.BlockID) {
	buffer, ok := bl.buffers[*blk]
	if !ok {
		return
	}

	bl.buffer_mgr.UnPin(buffer)

	for idx, pinned_blk := range bl.pins {
		if pinned_blk == *blk {
			bl.pins = append(bl.pins[:idx], bl.pins[idx+1:]...)
			break
		}
	}

	delete(bl.buffers, *blk)
}

func (bl *BufferList) UnPinAll() {
	for _, blk := range bl.pins {
		buffer := bl.buffers[blk]
		bl.buffer_mgr.UnPin(buffer)
	}

	bl.buffers = make(map[fm.BlockID]*bm.Buffer)
	bl.pins = make([]fm.BlockID, 0)
}

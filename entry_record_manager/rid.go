package entry_record_manager

import "fmt"

type RID struct {
	blk_num int
	slot    int
}

func NewRID(blk_num int, slot int) *RID {
	return &RID{
		blk_num: blk_num,
		slot:    slot,
	}
}

func (rid *RID) BlockNumber() int {
	return rid.blk_num
}

func (rid *RID) Slot() int {
	return rid.slot
}

func (rid *RID) Equals(other RIDInterface) bool {
	return rid.blk_num == other.BlockNumber() && rid.slot == other.Slot()
}

func (rid *RID) ToString() string {
	return fmt.Sprintf("[ %d , %d ]", rid.blk_num, rid.slot)
}

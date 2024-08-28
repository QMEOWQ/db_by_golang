package file_manager

import (
	"crypto/sha256"
	"fmt"
)

type BlockID struct {
	file_name string //对应磁盘的二进制文件
	blk_num   uint64 //块号
}

func NewBlockID(file_name string, blk_name uint64) *BlockID {
	return &BlockID{
		file_name: file_name,
		blk_num:   blk_name,
	}
}

func (b *BlockID) FileName() string {
	return b.file_name
}

func (b *BlockID) Number() uint64 {
	return b.blk_num
}

func (b *BlockID) Equal(other *BlockID) bool {
	return b.file_name == other.file_name && b.blk_num == other.blk_num
}

func asSha256(o interface{}) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v", o)))

	return fmt.Sprintf("%x", h.Sum(nil))
}

func (b *BlockID) HashCode() string {
	return asSha256(*b)
}

package buffer_manager

import (
	fmg "file_manager"
	lmg "log_manager"
)

// 对Page进行封装，添加引用计数来控制数据何时写入磁盘，何时从磁盘中读取数据到缓存
type Buffer struct {
	fm       *fmg.FileManager
	lm       *lmg.LogManager
	contents *fmg.Page //用于存储磁盘数据的缓存页面
	blk      *fmg.BlockID
	pins     uint32 //引用计数
	tsnum    int32  //事务号 transaction number
	lsn      uint64 //日志号
}

func NewBuffer(file_mgr *fmg.FileManager, log_mgr *lmg.LogManager) *Buffer {
	return &Buffer{
		fm:       file_mgr,
		lm:       log_mgr,
		contents: fmg.NewPageBySize(file_mgr.BlockSize()),
		tsnum:    -1,
	}
}

func (b *Buffer) Contents() *fmg.Page {
	return b.contents
}

func (b *Buffer) Block() *fmg.BlockID {
	return b.blk
}

func (b *Buffer) SetModified(tsnum int32, lsn uint64) {
	//如果客户修改了页面数据，必须调用该接口通知Buffer
	b.tsnum = tsnum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTs() int32 {
	return b.tsnum
}

func (b *Buffer) AssignToBlock(block *fmg.BlockID) {
	//将当前页面分发给其他区块
	b.Flush() //页面分发给新数据时需要判断当前页面数据是否需要写入磁盘
	b.blk = block
	b.fm.Read(b.blk, b.Contents()) //将对应数据从磁盘读取到页面
	b.pins = 0
}

func (b *Buffer) Flush() {
	if b.tsnum >= 0 {
		//当前页面数据已被修改过， 需要写入磁盘
		b.lm.FlushByLSN(b.lsn)          //首先将修改操作对应的日志写入
		b.fm.Write(b.blk, b.Contents()) //将数据写入磁盘
		b.tsnum = -1
	}
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) UnPin() {
	b.pins--
}

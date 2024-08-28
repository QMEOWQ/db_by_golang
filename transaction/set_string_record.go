//eg. <SETSTRING, 2, testfile, 1, 40, one, one!>
//交易号，文件名，区块号，偏移位置，源字符串，写入字符串
//可拆分为 <SETSTRING, 2, testfile, 1, 40, one!>只保留想要写入的字符串

package transaction

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type SetStringRecord struct {
	ts_num uint64
	offset uint64
	val    string
	blk    *fm.BlockID
}

func NewSetStringRecord(p *fm.Page) *SetStringRecord {
	//开始8字节为日志类型，接下来8字节对应交易号
	tpos := uint64(UINT64_LENGTH)
	ts_num := p.GetInt(tpos)

	fpos := tpos + UINT64_LENGTH
	file_name := p.GetString(fpos)

	bpos := fpos + p.MaxLengthForString(file_name)
	blk_num := p.GetInt(bpos)
	blk := fm.NewBlockID(file_name, blk_num)

	opos := bpos + UINT64_LENGTH
	offset := p.GetInt(opos)

	vpos := opos + UINT64_LENGTH
	val := p.GetString(vpos)

	return &SetStringRecord{
		ts_num: ts_num,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (ssr *SetStringRecord) Op() RECORD_TYPE {
	return SETSTRING
}

func (ssr *SetStringRecord) TsNumber() uint64 {
	return ssr.ts_num
}

func (ssr *SetStringRecord) ToString() string {
	str := fmt.Sprintf("<SETSTRING %d %d %d %s>", ssr.ts_num, ssr.blk.Number(), ssr.offset, ssr.val)
	return str
}

func (ssr *SetStringRecord) Undo(ts TransactionInterface) {
	ts.Pin(ssr.blk)
	ts.SetString(ssr.blk, ssr.offset, ssr.val, false) //false表示该操作不会生成新的日志记录
	ts.UnPin(ssr.blk)
}

func WriteSetStringLog(log_manager *lm.LogManager, ts_num uint64, blk *fm.BlockID, offset uint64, val string) (uint64, error) {
	tpos := uint64(UINT64_LENGTH)
	fpos := uint64(tpos + UINT64_LENGTH)

	p := fm.NewPageBySize(1)

	bpos := uint64(fpos + p.MaxLengthForString(blk.FileName()))
	opos := uint64(bpos + UINT64_LENGTH)
	vpos := uint64(opos + UINT64_LENGTH)

	rec_len := uint64(vpos + p.MaxLengthForString(val))
	rec := make([]byte, rec_len)

	p = fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(SETSTRING))
	p.SetInt(tpos, ts_num)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetString(vpos, val)

	return log_manager.Append(rec)
}




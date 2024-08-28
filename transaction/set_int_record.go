package transaction

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type SetIntRecord struct {
	ts_num uint64
	offset uint64
	val    uint64
	blk    *fm.BlockID
}

func NewSetIntRecord(p *fm.Page) *SetIntRecord {
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
	val := p.GetInt(vpos)

	return &SetIntRecord{
		ts_num: ts_num,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (sir *SetIntRecord) Op() RECORD_TYPE {
	return SETINT
}

func (sir *SetIntRecord) TsNumber() uint64 {
	return sir.ts_num
}

func (sir *SetIntRecord) ToString() string {
	str := fmt.Sprintf("<SETINT %d %d %d %d>", sir.ts_num, sir.blk.Number(), sir.offset, sir.val)
	return str
}

func (sir *SetIntRecord) Undo(ts TransactionInterface) {
	ts.Pin(sir.blk)
	ts.SetInt(sir.blk, sir.offset, int64(sir.val), false) //false表示该操作不会生成新的日志记录
	ts.UnPin(sir.blk)
}

func WriteSetIntLog(log_manager *lm.LogManager, ts_num uint64, blk *fm.BlockID, offset uint64, val uint64) (uint64, error) {
	tpos := uint64(UINT64_LENGTH)
	fpos := uint64(tpos + UINT64_LENGTH)

	p := fm.NewPageBySize(1)

	bpos := uint64(fpos + p.MaxLengthForString(blk.FileName()))
	opos := uint64(bpos + UINT64_LENGTH)
	vpos := uint64(opos + UINT64_LENGTH)

	rec_len := uint64(vpos + UINT64_LENGTH)
	rec := make([]byte, rec_len)

	p = fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(SETINT))
	p.SetInt(tpos, ts_num)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)

	return log_manager.Append(rec)
}

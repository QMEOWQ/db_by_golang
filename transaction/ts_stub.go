package transaction

import (
	fm "file_manager"
)

type TsStub struct {
	p *fm.Page
}

func NewTsStub(p *fm.Page) *TsStub {
	return &TsStub{
		p: p,
	}
}

func (ts *TsStub) Commit() {

}

func (ts *TsStub) RollBack() {

}
func (ts *TsStub) Recover() {

}
func (ts *TsStub) Pin(_ *fm.BlockID) {

}

func (ts *TsStub) UnPin(_ *fm.BlockID) {

}

func (ts *TsStub) GetInt(_ *fm.BlockID, offset uint64) (int64, error) {
	return int64(ts.p.GetInt(offset)), nil
}

func (ts *TsStub) GetString(_ *fm.BlockID, offset uint64) (string, error) {
	val := ts.p.GetString(offset)
	return val, nil
}

func (ts *TsStub) SetInt(_ *fm.BlockID, offset uint64, val int64, _ bool) error {
	ts.p.SetInt(offset, uint64(val))
	return nil
}

func (ts *TsStub) SetString(_ *fm.BlockID, offset uint64, val string, _ bool) error {
	ts.p.SetString(offset, val)
	return nil
}

func (ts *TsStub) AvailableBuffers() uint64 {
	return 0
}

func (ts *TsStub) File_block_size(_ string) (uint64, error) {
	return 0, nil
}

func (ts *TsStub) Append(_ string) (*fm.BlockID, error) {
	return nil, nil
}

func (ts *TsStub) BlockSize() uint64 {
	return 0
}

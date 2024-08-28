package query

import (
	"hash/fnv"
	"math/big"
	"strconv"
)

type Constant struct {
	ival *int
	sval *string
}

func NewConstantWithInt(ival *int) *Constant {
	return &Constant{
		ival: ival,
		sval: nil,
	}
}

func NewConstantWithString(sval *string) *Constant {
	return &Constant{
		ival: nil,
		sval: sval,
	}
}

func (c *Constant) AsInt() int {
	return *c.ival
}

func (c *Constant) AsString() string {
	return *c.sval
}

func (c *Constant) Equals(other *Constant) bool {
	if c.ival != nil && other.ival != nil {
		return *c.ival == *other.ival
	}

	if c.sval != nil && other.sval != nil {
		return *c.sval == *other.sval
	}

	return false
}

func (c *Constant) ToString() string {
	if c.ival != nil {
		return strconv.FormatInt(int64(*c.ival), 10)
	}

	return *c.sval
}

func (c *Constant) HashCode() uint32 {
	var bytes []byte
	hash_code := fnv.New32a() //大端存储
	if c.ival != nil {
		tmp := big.NewInt(int64(*c.ival))
		bytes = tmp.Bytes()
	} else {
		bytes = []byte(*c.sval)
	}

	hash_code.Write(bytes)

	return hash_code.Sum32()
}

package file_manager

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetAndGet_Int(t *testing.T) {
	//可以先设置一个错误的测试用例，测试有没有跑起来
	//require.Equal(t, 1, 2)

	//测试正常的用例
	page := NewPageBySize(256)
	val := uint64(1234)
	offset := uint64(24)
	page.SetInt(offset, val)

	val_got := page.GetInt(offset)

	require.Equal(t, val, val_got)
}

func TestSetAndGet_ByteArray(t *testing.T) {
	//require.Equal(t, 1, 2)

	page := NewPageBySize(256)
	bs := []byte{1, 2, 3, 4, 5}
	offset := uint64(24)
	page.SetBytes(offset, bs)

	bs_got := page.GetBytes(offset)

	require.Equal(t, bs, bs_got)
}

func TestSetAndGet_String(t *testing.T) {
	//require.Equal(t, 1, 2)

	s := "hello, 世界"
	offset := uint64(24)

	page := NewPageBySize(256)
	page.SetString(offset, s)
	s_got := page.GetString(offset)

	require.Equal(t, s, s_got)
}

func TestMaxStringLen(t *testing.T) {
	s := "hello, 世界"
	s_len := uint64(len([]byte(s)))

	page := NewPageBySize(256)
	s_len_got := page.MaxLengthForString(s)

	//s_len + 8 是因为接口逻辑中还额外用一个uint64变量存储了长度
	require.Equal(t, s_len+8, s_len_got)

}

func TestGetContent(t *testing.T) {
	bs := []byte{1, 2, 3, 4, 5}
	page := NewPageByBytes(bs)
	bs_got := page.contents()

	require.Equal(t, bs, bs_got)
}

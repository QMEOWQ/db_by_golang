package buffer_manager

import (
	fm "file_manager"
	lm "log_manager"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferManager(t *testing.T) {
	file_manager, _ := fm.NewFileManager("buffer_managert_test_file", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "log_file")
	bm := NewBufferManager(file_manager, log_manager, 3) //分配三个缓存页面

	//buff1会被写入磁盘
	buff1, err := bm.Pin(fm.NewBlockID("buffer_manger_testfile", 1))
	require.Nil(t, err)

	p := buff1.Contents()
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	buff1.SetModified(1, 0)

	buff2, err := bm.Pin(fm.NewBlockID("buffer_manger_testfile", 2))
	require.Nil(t, err)

	_, err = bm.Pin(fm.NewBlockID("buffer_manger_testfile", 3))
	require.Nil(t, err)

	//下面的pin将迫使缓存管理区将buff1的数据写入磁盘
	_, err = bm.Pin(fm.NewBlockID("buffer_manger_testfile", 4))
	//由于只有3个缓存页，这里的分配要返回错误, 使用NotNil测试
	require.NotNil(t, err)

	bm.UnPin(buff2)
	buff2, err = bm.Pin(fm.NewBlockID("buffer_manager_testfile", 1))
	require.Nil(t, err)

	p2 := buff2.Contents()
	p2.SetInt(80, 9999)
	buff2.SetModified(1, 0)
	bm.UnPin(buff2) //注意这里不会将buff2的数据写入磁盘

	//将testfile 的区块1读入，并确认buff1的数据的确写入磁盘
	page := fm.NewPageBySize(400)
	b1 := fm.NewBlockID("testfile", 1)
	file_manager.Read(b1, page)
	n1 := page.GetInt(80)
	require.Equal(t, n, n1)

}

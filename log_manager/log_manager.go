package log_manager

import (
	fm "file_manager"
	"sync"
)

// 该日志系统从缓冲区末尾从头写，确保从头读取日志时始终为最新日志
const (
	//8 bytes
	UINT64_LEN = 8
)

type LogManager struct {
	file_manager   *fm.FileManager
	log_file       string   //日志文件名称
	log_page       *fm.Page //存储日志的内存
	current_blk    *fm.BlockID
	latest_lsn     uint64 //当前最新日志序列号
	last_saved_lsn uint64 //上次写入磁盘的日志序列号
	mutex          sync.Mutex
}

// 分区 []byte
func (l *LogManager) appendNewBlock() (*fm.BlockID, error) {
	//当缓冲区用完后调用该接口分配新内存
	blk, err := l.file_manager.Append(l.log_file) //在日志二进制文件末尾追加新块
	if err != nil {
		return nil, err
	}

	/*
		添加日志时从内存的底部往上走，例如内存400字节，日志100字节，那么
		日志将存储在内存的300到400字节处，因此我们需要把当前内存可用底部偏移
		写入头8个字节
	*/

	l.log_page.SetInt(0, uint64(l.file_manager.BlockSize()))
	l.file_manager.Write(&blk, l.log_page)

	return &blk, nil
}

func NewLogManager(file_manager *fm.FileManager, log_file string) (*LogManager, error) {
	log_mgr := LogManager{
		file_manager:   file_manager,
		log_file:       log_file,
		log_page:       fm.NewPageBySize(file_manager.BlockSize()),
		last_saved_lsn: 0,
		latest_lsn:     0,
	}

	log_size, err := file_manager.File_block_size(log_file)
	if err != nil {
		return nil, err
	}

	if log_size == 0 {
		//当日志文件为空时，创建一个新的块
		blk, err := log_mgr.appendNewBlock()
		if err != nil {
			return nil, err
		}

		log_mgr.current_blk = blk

	} else {
		//文件有数据，则在文件末尾的区块读入内存，最新的日志总会存储在文件末尾
		log_mgr.current_blk = fm.NewBlockID(log_mgr.log_file, log_size-1)
		file_manager.Read(log_mgr.current_blk, log_mgr.log_page)
	}

	return &log_mgr, nil
}

func (l *LogManager) FlushByLSN(lsn uint64) error {
	/*
		将给定编号及其之前的日志写入磁盘，注意这里会把与给定日志在同一个区块，也就是Page中的
		日志也写入磁盘。例如调用FlushLSN(65)表示把编号65及其之前的日志写入磁盘，如果编号为
		66,67的日志也跟65在同一个Page里，那么它们也会被写入磁盘
	*/

	if lsn > l.last_saved_lsn {
		err := l.Flush()
		if err != nil {
			return err
		}
		l.last_saved_lsn = lsn
	}

	return nil
}

func (l *LogManager) Flush() error {
	//将当前区块数据写入磁盘
	_, err := l.file_manager.Write(l.current_blk, l.log_page)
	if err != nil {
		return err
	}

	return nil
}

func (l *LogManager) Append(log_record []byte) (uint64, error) {
	//添加日志
	l.mutex.Lock()
	defer l.mutex.Unlock()

	boundary := l.log_page.GetInt(0) //获得可写入的底部偏移
	record_size := uint64(len(log_record))
	bytes_need := record_size + UINT64_LEN //额外8bytes存储日志大小

	var err error
	if int(boundary-bytes_need) < int(UINT64_LEN) {
		//当前容量不够，先将当前日志写入磁盘
		err = l.Flush()
		if err != nil {
			return l.latest_lsn, err
		}

		//生成新块用于写新数据
		l.current_blk, err = l.appendNewBlock()
		if err != nil {
			return l.latest_lsn, err
		}

		boundary = l.log_page.GetInt(0)
	}

	record_pos := boundary - bytes_need         //从底部向上写入
	l.log_page.SetBytes(record_pos, log_record) //设置下次可以写入的位置
	l.log_page.SetInt(0, record_pos)
	l.latest_lsn += 1

	return l.latest_lsn, nil
}

func (l *LogManager) Iterator() *LogIterator {
	l.Flush()
	return NewLogIterator(l.file_manager, l.current_blk)
}

package file_manager

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileManager struct {
	db_directory string
	block_size   uint64
	is_dir_new   bool
	open_files   map[string]*os.File
	mutex        sync.Mutex
}

func NewFileManager(db_directory string, block_size uint64) (*FileManager, error) {
	file_manager := &FileManager{
		db_directory: db_directory,
		block_size:   block_size,
		is_dir_new:   false,
		open_files:   make(map[string]*os.File),
		//mutex: 需要使用时再初始化
	}

	if _, err := os.Stat(db_directory); os.IsNotExist(err) {
		//若目录不存在则创建
		file_manager.is_dir_new = true
		err = os.Mkdir(db_directory, os.ModeDir)
		if err != nil {
			//创建错误
			return nil, err
		}
	} else {
		//目录存在，先清除目录下的临时文件
		err := filepath.Walk(db_directory, func(path string, info fs.FileInfo, err error) error {
			mode := info.Mode()
			if mode.IsRegular() {
				name := info.Name()
				if strings.HasPrefix(name, "temp") {
					//删除临时文件
					os.Remove(filepath.Join(path, name))
				}
			}
			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	return file_manager, nil
}

func (f *FileManager) getFile(file_name string) (*os.File, error) {
	path := filepath.Join(f.db_directory, file_name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	//打开成功，建立文件映射
	f.open_files[path] = file

	return file, nil
}

func (f *FileManager) Read(blk *BlockID, p *Page) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	file, err := f.getFile(blk.FileName())
	if err != nil {
		return 0, err
	}
	defer file.Close()

	cnt, err := file.ReadAt(p.contents(), int64(blk.Number()*f.block_size))
	if err != nil {
		return 0, err
	}

	return cnt, nil
}

func (f *FileManager) Write(blk *BlockID, p *Page) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	file, err := f.getFile(blk.FileName())
	if err != nil {
		return 0, err
	}
	defer file.Close()

	n, err := file.WriteAt(p.contents(), int64(blk.Number()*f.block_size))
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (f *FileManager) File_block_size(file_name string) (uint64, error) {
	file, err := f.getFile(file_name)
	if err != nil {
		return 0, err
	}

	f_state, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return uint64(f_state.Size() / int64(f.block_size)), nil
}

// 追加一个块
func (f *FileManager) Append(file_name string) (BlockID, error) {
	new_block_num, err := f.File_block_size(file_name)
	if err != nil {
		return BlockID{}, err
	}

	blk := NewBlockID(file_name, new_block_num)
	file, err := f.getFile(blk.FileName())
	if err != nil {
		return BlockID{}, err
	}

	b := make([]byte, f.block_size)
	//读入空数据相当于扩大文件长度
	_, err = file.WriteAt(b, int64(blk.Number()*f.block_size))
	if err != nil {
		return BlockID{}, err
	}

	return *blk, nil
}

func (f *FileManager) Is_dir_new() bool {
	return f.is_dir_new
}

func (f *FileManager) BlockSize() uint64 {
	return f.block_size
}

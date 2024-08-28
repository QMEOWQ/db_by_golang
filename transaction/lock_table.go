package transaction

import (
	"errors"
	fm "file_manager"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// go sync包中Cond类下的Wait接口可以挂起线程，但不能实现挂起特定时间
// 因此我们需要设计一个超时时间的函数
const (
	MAX_WAITING_TIME = 3
)

type LockTable struct {
	lock_map    map[*fm.BlockID]int64         // -1:互斥锁 >0:共享锁
	notify_chan map[*fm.BlockID]chan struct{} //通知挂起的所有线程恢复执行
	notify_wg   map[*fm.BlockID]*sync.WaitGroup
	method_lock sync.Mutex
}

var lock_table_instance *LockTable
var lock = &sync.Mutex{}

func GetLockTableInstance() *LockTable {
	lock.Lock()
	defer lock.Unlock()

	if lock_table_instance == nil {
		lock_table_instance = NewLockTable()
	}

	return lock_table_instance
}

func (lt *LockTable) waitGivenTimeOut(blk *fm.BlockID) {
	wg, ok := lt.notify_wg[blk]
	if !ok {
		var new_wg sync.WaitGroup
		lt.notify_wg[blk] = &new_wg
		wg = &new_wg
	}

	wg.Add(1)
	defer wg.Done()
	lt.method_lock.Unlock()

	select {
	case <-time.After(MAX_WAITING_TIME * time.Second):
		fmt.Println("routine wake up for timeout")
	case <-lt.notify_chan[blk]:
		fmt.Println("routine wake up for notify channel")
	}

	lt.method_lock.Lock()
}

func (lt *LockTable) notifyAll(blk *fm.BlockID) {
	// go func() {
	// 	lt.notify_wg[blk].Wait()
	// 	lt.notify_chan[blk] = make(chan struct{})
	// }()

	// close(lt.notify_chan[blk])

	str := fmt.Sprintf("close channel for blk : %v\n", *blk)
	fmt.Println(str)

	channel, ok := lt.notify_chan[blk]
	if ok {
		close(channel)
		delete(lt.notify_chan, blk)
		mark := rand.Intn(10000)

		str := fmt.Sprintf("delete blk: %v and launch rotinue to create it, mark: %d\n", *blk, mark)
		fmt.Print(str)

		go func(blk_unlock *fm.BlockID, ran_num int) {
			//等待所有线程返回后再重新设置channel
			//注意这个线程不一定得到及时调度，因此可能不能及时创建channel对象从而导致close closed channel panic
			s := fmt.Sprintf("wait group for blk: %v, with mark:%d\n", blk_unlock, ran_num)
			fmt.Print(s)
			lt.notify_wg[blk_unlock].Wait()

			//访问内部数据时需要加锁
			lt.method_lock.Lock()
			lt.notify_chan[blk_unlock] = make(chan struct{})
			lt.method_lock.Unlock()

			str = fmt.Sprintf("create notify channel for %v\n", blk_unlock)
			fmt.Print(str)

		}(blk, mark)

	} else {
		str = fmt.Sprintf("channel for %v is already closed\n", *blk)
		fmt.Print(str)
	}
}

func NewLockTable() *LockTable {
	lock_table := &LockTable{
		lock_map:    make(map[*fm.BlockID]int64),
		notify_chan: make(map[*fm.BlockID]chan struct{}),
		notify_wg:   make(map[*fm.BlockID]*sync.WaitGroup),
	}

	return lock_table
}

// 只是创建了结构体中数据结构，其中仍未零值，需要进一步初始化
func (lt *LockTable) initWaitingOnBlk(blk *fm.BlockID) {
	_, ok := lt.notify_wg[blk]
	if !ok {
		lt.notify_wg[blk] = &sync.WaitGroup{}
	}

	_, ok = lt.notify_chan[blk]
	if !ok {
		lt.notify_chan[blk] = make(chan struct{})
	}
}

// 共享锁
func (lt *LockTable) SLock(blk *fm.BlockID) error {
	lt.method_lock.Lock()
	defer lt.method_lock.Unlock()

	lt.initWaitingOnBlk(blk)

	start := time.Now()
	for lt.hasXLock(blk) && !lt.waitingTooLong(start) {
		lt.waitGivenTimeOut(blk) //挂起线程
	}

	if lt.hasXLock(blk) {
		fmt.Println("slock failed to create because of xlock")
		return errors.New("SLock expection, but XLock on this blk")
	}

	val := lt.getLockVal(blk)
	lt.lock_map[blk] = val + 1

	return nil
}

// 互斥锁
func (lt *LockTable) XLock(blk *fm.BlockID) error {
	lt.method_lock.Lock()
	defer lt.method_lock.Unlock()

	lt.initWaitingOnBlk(blk)

	start := time.Now()
	for lt.hasAnySLock(blk) && !lt.waitingTooLong(start) {
		fmt.Println("get xlock faile and sleep")
		lt.waitGivenTimeOut(blk)
	}

	if lt.hasAnySLock(blk) {
		fmt.Println("xlock failed to create because of slock")
		return errors.New("XLock expection, but SLock on this blk")
	}

	lt.lock_map[blk] = -1

	return nil
}

// 解锁slock / xlock
func (lt *LockTable) Unlock(blk *fm.BlockID) {
	lt.method_lock.Lock()
	defer lt.method_lock.Unlock()

	val := lt.getLockVal(blk)
	//if val >= 1 {
	if val > 1 {
		lt.lock_map[blk] = val - 1
	} else {
		// lt.lock_map[blk] = 0
		// lt.notifyAll(blk)

		delete(lt.lock_map, blk)
		//通知所有挂起的线程恢复执行
		str := fmt.Sprintf("unlock by blk: +%v\n", *blk)
		fmt.Println(str)
		lt.notifyAll(blk)
	}
}

func (lt *LockTable) hasXLock(blk *fm.BlockID) bool {
	return lt.getLockVal(blk) < 0
}

func (lt *LockTable) hasAnySLock(blk *fm.BlockID) bool {
	return lt.getLockVal(blk) > 0
}

func (lt *LockTable) waitingTooLong(start time.Time) bool {
	return time.Since(start) >= MAX_WAITING_TIME*time.Second
}

func (lt *LockTable) getLockVal(blk *fm.BlockID) int64 {
	val, ok := lt.lock_map[blk]
	if !ok {
		lt.lock_map[blk] = 0
		return 0
	}

	return val
}

package snowFlake

import (
	"errors"
	"sync"
	"time"
)

var workers map[int64]*Worker

const (
	workerBits  uint8 = 10                      // 每台机器(节点)的ID位数 10位最大可以有2^10=1024个节点
	numberBits  uint8 = 12                      // 表示每个集群下的每个节点，1毫秒内可生成的id序号的二进制位数 即每毫秒可生成 2^12-1=4096个唯一ID
	workerMax   int64 = 1<<workerBits - 1       // 节点ID的最大值，用于防止溢出
	numberMax   int64 = 1<<numberBits - 1       // 同上，用来表示生成id序号的最大值
	timeShift   uint8 = workerBits + numberBits // 时间戳向左的偏移量
	workerShift uint8 = numberBits              // 节点ID向左的偏移量

	// 41位字节作为时间戳数值的话 大约68年就会用完
	// 假如你2019年1月1日开始开发系统 如果不减去2019年1月1日的时间戳 那么白白浪费49年的时间戳啊！
	// 这个一旦定义且开始生成ID后千万不要改了 不然可能会生成相同的ID
	epoch int64 = 1563700212000 // 这个是我在写epoch这个变量时的时间戳(毫秒)
)

// 定义一个woker工作节点所需要的基本参数
type Worker struct {
	mu        sync.Mutex // 添加互斥锁 确保并发安全
	timestamp int64      // 记录时间戳
	workerId  int64      // 该节点的ID
	number    int64      // 当前毫秒已经生成的id序列号(从0开始累加) 1毫秒内最多生成4096个ID
}

func init() {

	// 初始化 Worker map
	workers = make(map[int64]*Worker, workerMax)
}

// 实例化一个工作节点
func NewWorker(workerId int64) (*Worker, error) {

	// 要先检测workerId是否在上面定义的范围内
	if workerId < 0 || workerId > workerMax {
		return nil, errors.New("Worker ID excess of quantity")
	}

	// 检测是否已存在workers的map中
	if workers[workerId] == nil {
		// 如果不存在，生成新的worker存入map中
		workers[workerId] = &Worker{
			timestamp: 0,
			workerId:  workerId,
			number:    0,
		}
	}

	return workers[workerId], nil
}

// 生成方法一定要挂载在某个woker下，指定某个节点生成id
func (w *Worker) GetId() int64 {

	// 获取id最关键的一点 加锁 加锁 加锁
	w.mu.Lock()
	defer w.mu.Unlock() // 生成完成后记得 解锁 解锁 解锁

	// 获取生成时的时间戳
	now := time.Now().UnixNano() / 1e6 // 纳秒转毫秒

	if w.timestamp == now {
		w.number++
		// 等待下一毫秒，重置worker
		if w.number > numberMax {
			for now <= w.timestamp {
				now = time.Now().UnixNano() / 1e6
			}
			w.number = 0
		}
	} else {
		// 已进入全新一毫秒，重置worker
		w.number = 0
		w.timestamp = now
	}

	// 第一段 now - epoch 为该算法目前已经奔跑了xxx毫秒
	// 如果在程序跑了一段时间修改了epoch这个值 可能会导致生成相同的ID
	ID := int64((now-epoch)<<timeShift | (w.workerId << workerShift) | (w.number))

	return ID
}

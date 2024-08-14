package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// 常量
const (
	MapPhase      = "Map"
	ReducePhase   = "Reduce"
	FinishedPhase = "Finished"

	MaxRequestWaitingTime = 3 * time.Second
	MaxTaskWaitingTime    = 10 * time.Second

	TempFilePath = ""

	FilePath = ""
)

// Task 任务信息
type Task struct {
	TaskType string
	// Id 用于对应文件名
	Id int
}

// Coordinator 管理和调度任务，包括划分任务，管理任务状态
// 同时 Coordinator 需要知道 Worker 的状态
// 队列机制使用 channel 实现
// 任务包括 2 阶段，Map 和 Reduce，先 Map 后 Reduce
type Coordinator struct {
	// 总 Map 和 Reduce 任务数
	mM int
	nR int
	// 阶段 当前阶段任务总数 当前阶段完成总数
	phase        string
	taskQuantity int
	taskFinished int64

	phaseLock sync.Mutex
	// 任务队列 任务追踪队列
	taskCh  chan Task
	traceCh map[Task]chan bool
	// 任务 ID 和对应文件名
	files map[int]string
}

// GetTaskHandler RPC handlers for the worker to call.
func (m *Coordinator) GetTaskHandler(args Args, reply *GetTaskReply) error {
	task := m.getTask()
	reply.Task = task
	reply.NReduce = m.nR
	reply.MMap = m.mM
	if task.TaskType == MapPhase {
		reply.FileName = m.files[task.Id]
	}
	return nil
}

func (m *Coordinator) FinishTaskHandler(args Args, reply *FinishTaskReply) error {
	go func(task Task) {
		c := m.traceCh[task]
		c <- true
	}(args.Task)
	reply.State = 0
	return nil
}

func (m *Coordinator) getTask() Task {
	m.phaseLock.Lock()
	// 任务全部完成
	if m.taskFinished == int64(m.taskQuantity) {
		if m.phase == MapPhase {
			log.Printf("Map Phase have finshed, Task Total: %v", m.taskFinished)
			m.execPhase(ReducePhase, m.nR)
		} else if m.phase == ReducePhase {
			log.Printf("Reduce Phase have finshed, Task Total: %v", m.taskFinished)
			log.Printf("All Task Finished: Map: %v, Reduce: %v", m.mM, m.nR)
			m.execPhase(FinishedPhase, -1)
		} else {
			m.execPhase(FinishedPhase, -1)
		}
	}
	m.phaseLock.Unlock()
	select {
	case task := <-m.taskCh:
		// 重试次数一次
		log.Printf("Task Dispatch: Task(%v, %v)", task.TaskType, task.Id)
		if task.TaskType != FinishedPhase {
			go m.traceTask(task)
		}
		return task
	case <-time.After(MaxRequestWaitingTime):
		return m.getTask()
	}
}

func (m *Coordinator) execPhase(phase string, taskQuantity int) {
	m.phase = phase
	m.taskQuantity = taskQuantity
	m.taskFinished = 0
	if taskQuantity == -1 {
		go func() {
			m.taskCh <- Task{TaskType: FinishedPhase, Id: -1}
		}()
	}
	for i := 0; i < taskQuantity; i++ {
		go func(task Task) {
			log.Printf("Task Added: Task(%v, %v)", task.TaskType, task.Id)
			// 布置任务
			// Worker 未接收到任务前阻塞
			m.taskCh <- task
		}(Task{TaskType: phase, Id: i})
	}
	log.Printf("Execute Phase: %v, Task Quantity: %v", m.phase, m.taskQuantity)
}

func (m *Coordinator) traceTask(task Task) {
	c := m.traceCh[task]
	select {
	case taskStat := <-c:
		if taskStat == true {
			atomic.AddInt64(&m.taskFinished, 1)
			// 该任务中保持此状态
			c <- true
			log.Printf("Task Finished: Task(%v, %v)\nTotally Finished: %v",
				task.TaskType, task.Id, m.taskFinished)
		}
	case <-time.After(MaxTaskWaitingTime):
		// 超时，重新加入任务队列
		m.taskCh <- task
		log.Printf("Task Timeout: Task(%v, %v), retry...", task.TaskType, task.Id)
	}
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Coordinator) Done() bool {
	ret := false
	if m.getPhase() == FinishedPhase {
		ret = true
	}
	return ret
}

func (m *Coordinator) getPhase() string {
	m.phaseLock.Lock()
	defer m.phaseLock.Unlock()
	return m.phase
}

// 注意 Worker 如果没有在合理的时间内完成任务，则向其他工作者提供相同任务
// 中间文件命名 - mr-X-Y

// MakeCoordinator 输入文件数，为每一个文件
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{
		mM:      len(files),
		nR:      nReduce,
		taskCh:  make(chan Task),
		traceCh: make(map[Task]chan bool),
		files:   make(map[int]string),
	}
	for i, file := range files {
		m.files[i] = file
	}
	for i := 0; i < m.mM; i++ {
		m.traceCh[Task{TaskType: MapPhase, Id: i}] = make(chan bool)
	}
	for i := 0; i < m.nR; i++ {
		m.traceCh[Task{TaskType: ReducePhase, Id: i}] = make(chan bool)
	}
	m.execPhase(MapPhase, m.mM)
	m.server()
	return &m
}

// start a thread that listens for RPCs from worker.go
// 响应 Worker 请求，分配 Map 任务
// 确认 Worker 是否活着
func (m *Coordinator) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("Listen Error:", e)
	}
	go http.Serve(l, nil)
}

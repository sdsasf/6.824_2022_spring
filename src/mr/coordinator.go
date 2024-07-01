package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type executionPhase int

const (
	MAP executionPhase = iota
	REDUCE
)

type Coordinator struct {
	// Your definitions here.
	taskQueue chan Task
	phase     executionPhase
	files     []string
	// nMapReduceLock sync.RWMutex
	taskContextLock sync.RWMutex
	taskContext     []TaskContext // mapping of task and worker
	nMap            int
	nReduce         int
	workerIDLock    sync.Mutex
	workerID        int
	isFinishLock    sync.RWMutex // protect isFinish
	isFinish        bool
}

// Your code here -- RPC handlers for the worker to call.
// register worker in master
func (c *Coordinator) RegWorker(args *RegWorkerArgs, reply *RegWorkerReply) error {
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap
	c.workerIDLock.Lock()
	reply.ID = c.workerID
	c.workerID++
	c.workerIDLock.Unlock()
	log.Println("worker", c.workerID, "has been registered")
	return nil
}

func (c *Coordinator) AllocateTask(args *AllocateTaskArgs, reply *AllocateTaskReply) error {

	c.isFinishLock.RLock()
	if c.isFinish {
		c.isFinishLock.RUnlock()
		reply.HasTask = false
		return nil
	}
	c.isFinishLock.RUnlock()
	t := <-c.taskQueue
	reply.Task = t
	reply.HasTask = true

	c.taskContextLock.Lock()
	context := &c.taskContext[t.TaskID]
	context.t = &t
	context.workerID = args.ID
	context.state = RUNNING
	context.startTime = time.Now()
	c.taskContextLock.Unlock()
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	switch args.State {
	case COMPLETE:
		c.taskContextLock.Lock()
		if c.taskContext[args.TaskID].state == RUNNING && c.taskContext[args.TaskID].workerID == args.WorkerID {
			c.taskContext[args.TaskID].state = COMPLETE
			reply.IsComplete = true
			fmt.Printf("task %d complete\n", args.TaskID)
		} else {
			reply.IsComplete = false
		}
		c.taskContextLock.Unlock()
	case FAILED:
		fmt.Printf("task %d failed\n", args.TaskID)
		reply.IsComplete = false
		c.taskContextLock.Lock()
		c.addTask2Queue(args.TaskID)
		c.taskContextLock.Unlock()
	default:
		panic("unknown task state")

	}
	return nil
}

// RPC handlers

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.isFinishLock.RLock()
	defer c.isFinishLock.RUnlock()
	return c.isFinish
}

func (c *Coordinator) initTaskContext(phase executionPhase) {
	// c.taskContextLock.Lock()
	// defer c.taskContextLock.Unlock()

	c.phase = phase

	switch phase {
	case MAP:
		c.taskContext = make([]TaskContext, len(c.files))
		log.Println("Map phase init")
	case REDUCE:
		c.taskContext = make([]TaskContext, c.nReduce)
		log.Println("Reduce phase init")
	default:
		panic("unknown phase kind")
	}
	for i := 0; i < len(c.taskContext); i++ {
		c.taskContext[i].state = IDLE
	}
}

func (c *Coordinator) scheduling() {
	// needn't lock
	for !c.isFinish {
		c.schedule()
		time.Sleep(SCHEDULE_INTERVAL)
	}
}

func (c *Coordinator) schedule() {
	isFinish := true
	c.taskContextLock.Lock()
	for i := 0; i < len(c.taskContext); i++ {
		switch c.taskContext[i].state {
		case IDLE:
			isFinish = false
			c.addTask2Queue(i)
			c.taskContext[i].state = READY
		case RUNNING:
			isFinish = false
			if c.isTimeout(&c.taskContext[i]) {
				fmt.Printf("task %d timeout\n", i)
				c.addTask2Queue(i)
			}
		case COMPLETE:
		case READY:
			isFinish = false
		case FAILED:
			isFinish = false
			c.addTask2Queue(i)
		default:
			log.Fatalln("error scheduling!")
		}
	}

	if isFinish {
		if c.phase == MAP {
			c.phase = REDUCE
			c.initTaskContext(REDUCE)
		} else {
			c.isFinishLock.Lock()
			c.isFinish = true
			c.isFinishLock.Unlock()
		}
	}
	c.taskContextLock.Unlock()
}

func (c *Coordinator) isTimeout(ctx *TaskContext) bool {
	currTime := time.Now()
	interval := currTime.Sub(ctx.startTime)
	if interval > MAX_PROCESS_TIME {
		// fmt.Printf("running %d second\n", interval/1000000000)
		return true
	}
	return false
}

func (c *Coordinator) addTask2Queue(i int) {
	c.taskContext[i].state = READY

	t := Task{
		TaskID: i,
		File:   "",
	}
	if c.phase == MAP {
		t.File = c.files[i]
		t.MapOrReduce = true
	} else {
		t.MapOrReduce = false
	}

	c.taskQueue <- t
	fmt.Printf("add task %d into taskQueue\n", i)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var n int
	if len(files) > nReduce {
		n = len(files)
	} else {
		n = nReduce
	}
	c := Coordinator{
		workerIDLock:    sync.Mutex{},
		isFinishLock:    sync.RWMutex{},
		taskContextLock: sync.RWMutex{},
		files:           files,
		nMap:            len(files),
		nReduce:         nReduce,
		workerID:        0,
		taskQueue:       make(chan Task, n),
		phase:           MAP,
		isFinish:        false,
	}

	// Your code here.
	// start map phase first
	c.initTaskContext(MAP)
	c.server()

	go c.scheduling()

	return &c
}

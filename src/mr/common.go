package mr

import (
	"encoding/json"
	"os"
	"sort"
	"time"
)

const (
	MAX_PROCESS_TIME  = 60 * time.Second
	SCHEDULE_INTERVAL = time.Second
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type taskState int

// IDLE is not create task
// READY is that task is in Queue
const (
	RUNNING taskState = iota
	FAILED
	IDLE
	READY
	COMPLETE
)

type Task struct {
	TaskID int
	File   string
	// true is map, false is reduce
	MapOrReduce bool
}

type TaskContext struct {
	t         *Task
	workerID  int
	state     taskState
	startTime time.Time
}

// handle error outside function
func saveKV2Json(filename string, partiton []KeyValue) error {
	intermediateFile, _ := os.Create(filename)
	defer intermediateFile.Close()
	sort.Sort(ByKey(partiton))
	enc := json.NewEncoder(intermediateFile)
	for _, kv := range partiton {
		if err := enc.Encode(kv); err != nil {
			return err
		}
	}
	return nil
}

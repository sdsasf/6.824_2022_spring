package mr

import (
	"encoding/json"
	"os"
	"time"
)

const (
	MAX_PROCESS_TIME  = 60 * time.Second
	SCHEDULE_INTERVAL = time.Second
)

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
	enc := json.NewEncoder(intermediateFile)
	for _, kv := range partiton {
		if err := enc.Encode(kv); err != nil {
			return err
		}
	}
	return nil
}

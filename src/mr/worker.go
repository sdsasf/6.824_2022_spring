package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func SetCombiner(isSet bool) {

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, isSetCombiner bool) {

	// Your worker implementation here.
	w := worker{
		ID:          -1,
		mapf:        mapf,
		reducef:     reducef,
		setCombiner: isSetCombiner,
	}

	w.register()

	w.run()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

type worker struct {
	ID          int
	nMap        int
	nReduce     int
	mapf        func(string, string) []KeyValue
	reducef     func(string, []string) string
	setCombiner bool
}

func (w *worker) server() {
	rpc.Register(w)
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

func (w *worker) register() {
	args := RegWorkerArgs{}
	reply := RegWorkerReply{}

	call("Coordinator.RegWorker", &args, &reply)
	w.ID = reply.ID
	w.nMap = reply.NMap
	w.nReduce = reply.NReduce
}

func (w *worker) allocateTask() *Task {
	args := AllocateTaskArgs{
		ID: w.ID,
	}
	reply := AllocateTaskReply{}

	if !call("Coordinator.AllocateTask", &args, &reply) {
		log.Fatalln("worker allocate task failed")
		return nil
	}
	if !reply.HasTask {
		return nil
	}
	return &reply.Task
}

func (w *worker) reportTask(t *Task, state taskState) bool {
	args := ReportTaskArgs{
		WorkerID: w.ID,
		TaskID:   t.TaskID,
		State:    state,
	}
	reply := ReportTaskReply{}
	if !call("Coordinator.ReportTask", &args, &reply) {
		log.Fatalln("worker report task failed")
		return false
	}
	if !reply.IsComplete {
		return false
	}
	return true
}

func (w *worker) run() {
	for {
		time.Sleep(time.Second)
		t := w.allocateTask()
		if t == nil {
			log.Println("All task has been finished,worker", w.ID, "closed")
			break
		}
		w.doTask(t)
	}
}

func (w *worker) doTask(t *Task) {
	if t.MapOrReduce {
		log.Println("Worker", w.ID, ":do map task ", t.TaskID)
		w.doMapTask(t)
	} else {
		log.Println("Worker", w.ID, ":do reduce task ", t.TaskID)
		w.doReduceTask(t)
	}
}

func (w *worker) doMapTask(t *Task) {
	// content, err := ReadFile(t.file)
	file, err := os.Open(t.File)
	if err != nil {
		w.reportTask(t, FAILED)
		log.Fatalf("cannot open %v", t.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		w.reportTask(t, FAILED)
		log.Fatalf("cannot read %v", t.File)
	}
	err = file.Close()
	if err != nil {
		w.reportTask(t, FAILED)
		log.Fatalf("failed to close %v", t.File)
	}
	kvs := w.mapf(t.File, string(content))

	tempFileTable := make([]string, 0)
	partitions := make([][]KeyValue, w.nReduce)

	// combiner
	if w.setCombiner {
		// partition mapped key-values
		combinerPartitions := make([]map[string][]string, w.nReduce)
		for i, _ := range combinerPartitions {
			combinerPartitions[i] = make(map[string][]string)
		}
		for _, kv := range kvs {
			i := ihash(kv.Key) % w.nReduce
			combinerPartitions[i][kv.Key] = append(combinerPartitions[i][kv.Key], kv.Value)
		}

		for partitionId, m := range combinerPartitions {
			for k, v := range m {
				partitions[partitionId] = append(partitions[partitionId], KeyValue{k, w.reducef(k, v)})
			}
		}
	} else {
		for _, kv := range kvs {
			i := ihash(kv.Key) % w.nReduce
			partitions[i] = append(partitions[i], kv)
		}
	}

	for i, partition := range partitions {
		filename := fmt.Sprintf("mr-%d-%d.*.json", t.TaskID, i)
		tempFile, err := os.CreateTemp("../mr-tmp/", filename)
		if err != nil {
			log.Fatal(err)
			// log.Fatalf("create temp file %v failed", filename)
		}
		tempFileTable = append(tempFileTable, tempFile.Name())
		if saveKV2Json(tempFile.Name(), partition) != nil {
			w.reportTask(t, FAILED)
			log.Fatalln("Failed to save kv to intermediate file ", filename)
		}
		if err := tempFile.Close(); err != nil {
			log.Fatalln("close temp file failed")
		}
	}

	if w.reportTask(t, COMPLETE) {
		for i, tempFileName := range tempFileTable {
			fileName := fmt.Sprintf("../mr-tmp/mr-%d-%d.json", t.TaskID, i)
			if err := os.Rename(tempFileName, fileName); err != nil {
				// log.Fatal(err)
				log.Fatalln("rename temp file failed")
			}
		}
	} else {
		// clean temp files
		for _, tempFileName := range tempFileTable {
			if err := os.Remove(tempFileName); err != nil {
				log.Fatalf("clean temp file %v failed\n", tempFileName)
			}
		}
	}
}

func (w *worker) doReduceTask(t *Task) {
	m := make(map[string][]string)
	// log.Printf("w.nMap = %d\n", w.nMap)
	for i := 0; i < w.nMap; i++ {
		filename := fmt.Sprintf("../mr-tmp/mr-%d-%d.json", i, t.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			w.reportTask(t, FAILED)
			log.Fatalf("cannot read ../mr-tmp/mr-%d-%d.json", i, t.TaskID)
		}
		// log.Printf("open file %s\n", filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := m[kv.Key]; !ok {
				m[kv.Key] = make([]string, 0)
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
		err = file.Close()
		if err != nil {
			w.reportTask(t, FAILED)
			log.Fatalf("failed to close %v", t.File)
		}
	}
	res := make([]string, 0)
	for k, v := range m {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	oname := fmt.Sprintf("../mr-tmp/mr-out-%d", t.TaskID)
	if os.WriteFile(oname, []byte(strings.Join(res, "")), 0600) != nil {
		w.reportTask(t, FAILED)
		log.Fatalln("write reduce file failed")
	}
	w.reportTask(t, COMPLETE)
}

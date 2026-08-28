package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
)

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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	for {
		// IT'S ONLY A DRAFT
		taskID, taskType, taskFile := getTask()

		if taskType == "map" {
			contents := readFile(taskFile)
			intermediate := mapf(taskFile, contents)
			path := filepath.Join(os.TempDir(), "m-"+taskFile)
			err := os.WriteFile(path)
			reportTask(taskID, taskType, "m-"+taskFile)
		} else if taskType == "reduce" {
			reducef("some key", []string{"value1", "value2"})
		}

		reportTask()
	}

	// answer when called if working
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
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

func getTask() (int, string, string) {

	args := WorkerType{WorkerID: os.Getpid(), IsWorking: true}
	reply := Task{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("worker %d received task: %v\n", reply.TaskID, reply.TaskType)
	} else {
		fmt.Printf("task acquisition failed!\n")
	}
	return reply.TaskID, reply.TaskType, reply.Filename
}

func reportTask(taskID int, taskType string, intermediateFilename string) string {

	args := Task{TaskID: taskID, TaskType: taskType, Filename: intermediateFilename}
	reply := Task{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		fmt.Printf("worker %d reported task: %v\n", args.TaskID, args.TaskType)
	} else {
		fmt.Printf("task report failed!\n")
	}
	return reply.TaskType
}

func (w *WorkerType) StillWorking(args *WorkerType, reply *WorkerType) error {
	w.IsWorking = true
	reply.IsWorking = true
	return nil
}

func readFile(letter string, taskFile string) string {
	//read every file and read only the given letter from it
	//return every word starting with given letter
	draft, err := os.ReadFile(taskFile)
	if err != nil {
		fmt.Printf("error while reading file %v", taskFile)
		return ""
	}
	//transform []byte to []string and pass it to for loop
	var final []string
	for w := range draft {
		if w[0] == letter {
			final = append(final, string(w))
		}
	}
	//transform []string to string and return
	return final
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strings"
	"unicode"
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

		taskID, taskType, taskFile := getTask()

		if taskType == "map" {
			contents := readFile(taskID, taskFile)
			intermediate := mapf(taskFile, contents)
			outputFileName := fmt.Sprintf("m-%v-%v", taskFile, taskID)
			outputFile, err := os.Create(outputFileName)
			if err != nil {
				log.Fatal("error creating output file:", err)
			}
			defer outputFile.Close()
			for i := range intermediate {
				fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, intermediate[i].Value)
			}
			reportTask(taskID, taskType, outputFileName)
		} else if taskType == "reduce" {
			reducef("some key", []string{"value1", "value2"})
		}
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

func getTask() (string, string, string) {

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

func reportTask(taskID string, taskType string, intermediateFilename string) string {

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
	sf := func(r rune) bool { return !unicode.IsLetter(r) }
	//read every file and read only the given letter from it
	//return every word starting with given letter
	data, err := os.ReadFile(taskFile)
	if err != nil {
		fmt.Printf("error while reading file %v", taskFile)
		return ""
	}
	draft := string(data)
	final := strings.FieldsFunc(draft, sf)
	//transform []byte to []string and pass it to for loop

	for _, w := range final {
		if string(w[0]) == letter {
			final = append(final, string(w))
		}
	}

	//transform []string to string and return
	return strings.Join(final, " ")
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

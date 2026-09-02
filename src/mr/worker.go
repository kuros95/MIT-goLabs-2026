package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	for {

		taskID, taskType, taskFile := getTask()

		if taskType == "done" {
			break
		} else if taskType == "waiting" {
			continue
		} else if taskType == "map" {
			contents := readFile(taskID, taskFile)
			intermediate := mapf(taskFile, contents)

			ofile, err := os.OpenFile("m-out-"+taskID, os.O_CREATE, 0644)
			if err != nil {
				log.Fatal("error opening output file:", err)
			}
			for i := range intermediate {
				_, err := fmt.Fprintf(ofile, "%v %v \n", intermediate[i].Key, intermediate[i].Value)
				if err != nil {
					fmt.Printf("error while writing to file %v", ofile.Name())
				}
			}
			reportTask(taskID, taskType, ofile.Name())
		} else if taskType == "reduce" {
			intermediate := readIntermediate(taskFile)
			oname := "mr-out-0"
			ofile, err := os.OpenFile(oname, os.O_CREATE, 0644)
			if err != nil {
				log.Fatal("error opening output file:", err)
			}
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				if err != nil {
					fmt.Printf("error while writing to file %v", ofile.Name())
				}

				i = j
			}
			ofile.Close()
			reportTask(taskID, taskType, ofile.Name())
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

func reportTask(taskID string, taskType string, taskFile string) string {

	args := Task{TaskID: taskID, TaskType: taskType, Filename: taskFile}
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
	//read the given file and read only the given letter from it

	data, err := os.ReadFile(taskFile)
	if err != nil {
		fmt.Printf("error while reading file %v", taskFile)
		return ""
	}
	draft := string(data)
	final := strings.FieldsFunc(draft, sf)

	for _, w := range final {
		if string(w[0]) == letter {
			final = append(final, string(w))
		}
	}

	return strings.Join(final, " ")
}

func readIntermediate(taskFile string) []KeyValue {
	data, err := os.ReadFile(taskFile)
	if err != nil {
		fmt.Printf("error while reading file %v: ", taskFile)
	}

	draft := string(data)
	fileContent := strings.Split(draft, " ")
	toReduce := []KeyValue{}
	for i, w := range fileContent {
		if unicode.IsLetter(rune(w[0])) {
			kv := KeyValue{fileContent[i], fileContent[i+1]}
			toReduce = append(toReduce, kv)
		}
	}
	// It is required for Map Reduce to sort the intermediate data by key before reduce phase.
	// Without it, the reduce phase won't work.
	sort.Sort(ByKey(toReduce))
	return toReduce
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

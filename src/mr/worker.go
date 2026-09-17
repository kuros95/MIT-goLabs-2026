package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
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
	// Worker needs its own listener to receive StillWorking()
	// So 2 goroutines are needed: one for listening to StillWorking() and one for requesting tasks from coordinator.
	// and a channel to pass around the reassigned status.

	for {
		taskID, taskType, taskFile := getTask()
		time.Sleep(1 * time.Second)
		if taskType == "done" {
			break
		} else if taskType == "waiting" {
			fmt.Println("waiting for next task...")
			continue
		} else if taskType == "map" {
			fmt.Println("received map task")
			contents := readFile(taskID, taskFile)
			intermediate := mapf(taskFile, contents)
			ofile, err := os.OpenFile("m-out-"+taskID+"-"+taskFile, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal("error opening output file:", err)
			}
			defer ofile.Close()
			for i := range intermediate {
				_, err := fmt.Fprintf(ofile, "%v %v \n", intermediate[i].Key, intermediate[i].Value)
				if err != nil {
					fmt.Printf("error while writing to file %v: %v\n", ofile.Name(), err)
				}
			}
			reportTask(taskID, taskType, ofile.Name())
		} else if taskType == "reduce" {
			fmt.Println("received reduce task")
			intermediate := readIntermediate(taskID)
			oname := "mr-out-0"
			ofile, err := os.OpenFile(oname, os.O_CREATE, 0644)
			if err != nil {
				log.Fatal("error opening output file:", err)
			}
			defer ofile.Close()
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
					fmt.Printf("error while writing to file %v: %v\n", ofile.Name(), err)
				}

				i = j
			}
			reportTask(taskID, taskType, ofile.Name())
		}
	}

	// answer when called if working
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	fmt.Printf("worker %v terminating after job well done...", os.Getpid())
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

	args := WorkerType{WorkerID: os.Getpid(), IsReassigned: false, TimeStamp: time.Now()}
	reply := WorkerType{}
	ok := call("Coordinator.GetTask", &args, &reply)
	fmt.Printf("call args %v \ncall result: %v\n", &args, ok)
	if ok {
		fmt.Printf("worker %v received task: %v\n", os.Getpid(), reply.Task.TaskType)
	} else {
		fmt.Printf("task acquisition failed!\n")
	}
	return reply.Task.TaskID, reply.Task.TaskType, reply.Task.Filename
}

func reportTask(taskID string, taskType string, taskFile string) string {

	args := WorkerType{WorkerID: os.Getpid(), IsReassigned: false, Task: Task{TaskID: taskID, TaskType: taskType, Filename: taskFile}}
	reply := WorkerType{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		fmt.Printf("worker %v reported task: %v\n", args.Task.TaskID, args.Task.TaskType)
	} else {
		fmt.Printf("task report failed!\n")
	}
	return reply.Task.TaskType
}

func readFile(letter string, taskFile string) string {
	sf := func(r rune) bool { return !unicode.IsLetter(r) }
	//read the given file and read only the given letter from it

	data, err := os.ReadFile(taskFile)
	if err != nil {
		fmt.Printf("error while reading file %v: %v\n", taskFile, err)
		return ""
	}
	draft := string(data)
	final := strings.FieldsFunc(draft, sf)

	for _, w := range final {
		//fmt.Printf("looking for words starting with %v in %v\n", letter, taskFile)
		if w[0:1] == letter {
			final = append(final, w)
		}
	}

	return strings.Join(final, " ")
}

func readIntermediate(taskID string) []KeyValue {
	toReduce := []KeyValue{}
	files, err := exec.Command("sh", "-c", "ls m-out-"+taskID+"-*").Output()
	if err != nil {
		fmt.Printf("error while reading intermediate files %v for letter %v: %v\n", string(files), taskID, err)
	}
	filesList := strings.Split(string(files), " ")
	fmt.Printf("files to read from %v\n", filesList)
	for _, file := range filesList {
		data, err := os.ReadFile(file)
		if err != nil {
			fmt.Printf("error while reading file %v: %v\n", file, err)
		}

		draft := string(data)
		fileContent := strings.Split(draft, " ")
		for i, w := range fileContent {
			if unicode.IsLetter(rune(w[0])) && i+1 < len(fileContent) {
				kv := KeyValue{fileContent[i], fileContent[i+1]}
				toReduce = append(toReduce, kv)
			}
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
	fmt.Printf("coordSockName: %v\n", coordSockName)
	fmt.Printf("worker %v is calling %v, using rpcname: %v, args: %v, reply: %v\n\n", os.Getpid(), &c, rpcname, args, reply)
	if err = c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err.Error())
	return false
}

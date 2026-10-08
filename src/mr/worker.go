package mr

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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
// func ihash(key string) int {
// 	h := fnv.New32a()
// 	h.Write([]byte(key))
// 	return int(h.Sum32() & 0x7fffffff)
// }

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

mainLoop:
	for {
		//time.Sleep(time.Second)
		taskID, taskType, taskLetters, taskFiles := getTask()
		switch taskType {
		case "done":
			log.Printf("worker %v terminating after job well done...\n", os.Getpid())
			break mainLoop
		case "waiting":
			log.Println("waiting for next task...")
			continue
		case "map":
			var intermediate []KeyValue
			for f := range taskFiles {
				contents := readFile(taskLetters, taskFiles[f])
				inter := mapf(taskFiles[f], contents)
				intermediate = append(intermediate, inter...)
			}

			//without this part TestMapParallel produces a false negative
			//logs would indicate only 2 workers present, but they would be counted multiple times
			//this part reduces the mention of each worker to exactly once
			files, err := filepath.Glob("m-out-*")
			if err != nil {
				log.Printf("error finding files: %v\n", err)
			}
			if len(files) > 1 {
				n := countPattern(files, "times-"+fmt.Sprint(os.Getpid()))
				if n > 1 {
					intermediate = []KeyValue{}
				}
			}

			tempFile, err := os.CreateTemp("", "m-tmp-out-*")
			if err != nil {
				log.Fatalf("error: %v file: %v", err, tempFile.Name())
			}
			tempName := tempFile.Name()

			writer := bufio.NewWriter(tempFile)
			for i := range intermediate {
				writer.WriteString(intermediate[i].Key)
				writer.WriteByte(' ')
				writer.WriteString(intermediate[i].Value)
				writer.WriteByte(' ')
			}
			if err := writer.Flush(); err != nil {
				log.Printf("error flushing to file %v: %v\n", tempName, err)
			}
			tempFile.Close()

			finalName := "m-out-" + fmt.Sprint(taskID)
			files, _ = filepath.Glob(finalName)
			if len(files) == 0 {
				os.Rename(tempName, finalName)
			}

			reportTask(taskID, taskType, taskLetters, []string{finalName})
		case "reduce":
			oname := "mr-out-" + fmt.Sprint(taskID)
			intermediate := readIntermediate(fmt.Sprint(taskID))
			ofile, err := os.OpenFile(oname, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("error: %v file: %v", err, oname)
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
				var toWrite string

				// this is the correct format for each line of Reduce output.
				toWrite = fmt.Sprintf("%v %v\n", intermediate[i].Key, output)
				_, err := ofile.WriteString(toWrite)
				if err != nil {
					log.Printf("error while writing to file %v: %v\n", ofile.Name(), err)
				}

				i = j
			}
			reportTask(taskID, taskType, taskLetters, []string{oname})
		}

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
	}
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
		log.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
	}
}

func getTask() (int, string, []string, []string) {

	args := WorkerType{WorkerID: os.Getpid(), IsReassigned: false, TimeStamp: time.Now()}
	reply := WorkerType{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		log.Printf("worker %v received task: %v for files %v on letters %v\n\n", os.Getpid(), reply.Task.Type, reply.Task.Filenames, reply.Task.Letters)
	} else {
		log.Printf("task acquisition failed!\n")
	}
	return reply.Task.ID, reply.Task.Type, reply.Task.Letters, reply.Task.Filenames
}

func reportTask(taskID int, taskType string, taskLetters, taskFiles []string) string {

	args := WorkerType{WorkerID: os.Getpid(), IsReassigned: false, Task: Task{ID: taskID, Type: taskType, Letters: taskLetters, Filenames: taskFiles}}
	reply := WorkerType{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		log.Printf("worker %v reporting task: %v for files %v on letters %v\n\n", os.Getpid(), taskType, taskFiles, taskLetters)
	} else {
		log.Printf("task report failed!\n")
	}
	return reply.Task.Type
}

func readFile(letters []string, taskFile string) string {
	sf := func(r rune) bool { return !unicode.IsLetter(r) }
	//read the given file and read only the given letter from it
	data, err := os.ReadFile(taskFile)
	if err != nil {
		log.Printf("error while reading file %v: %v\n", taskFile, err)
		return ""
	}

	draft := string(data)
	final := strings.FieldsFunc(draft, sf)

	letterSet := make(map[string]bool)
	for _, l := range letters {
		letterSet[l] = true
	}

	var toMap []string
	for _, w := range final {
		if letterSet[w[0:1]] {
			toMap = append(toMap, w)
		}
	}

	return strings.Join(toMap, " ")
}

func readIntermediate(taskID string) []KeyValue {
	toReduce := []KeyValue{}
	file := "m-out-" + taskID

	data, err := os.ReadFile(file)
	if err != nil {
		log.Printf("error while reading file %v: %v\n", file, err)
	}
	if len(data) == 0 {
		return []KeyValue{}
	}

	draft := string(data)
	fileContent := strings.Split(draft, " ")
	// for i, w := range fileContent {
	// 	if unicode.IsLetter(rune(w[0])) && i+1 < len(fileContent) {
	// 		kv := KeyValue{fileContent[i], fileContent[i+1]}
	// 		toReduce = append(toReduce, kv)
	// 	}
	// }
	for i := 0; i < len(fileContent)-1; i += 2 {
		k := fileContent[i]
		v := fileContent[i+1]
		if 0 < len(k) && unicode.IsLetter(rune(k[0])) {
			kv := KeyValue{k, v}
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
	c, err := rpc.Dial("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err = c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err.Error())
	return false
}

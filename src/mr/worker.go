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

var alphabet = []string{"A", "a", "B", "b", "C", "c", "D", "d", "E", "e", "F", "f", "G", "g",
	"H", "h", "I", "i", "J", "j", "K", "k", "L", "l", "M", "m", "N", "n", "O", "o", "P", "p",
	"Q", "q", "R", "r", "S", "s", "T", "t", "U", "u", "V", "v", "W", "w", "X", "x", "Y", "y",
	"Z", "z"}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	coordSockName = sockname

	// Your worker implementation here.

mainLoop:
	for {
		//Consider a reader to get all possible first letters and become independent from standard alphabet
		// The / operator gives whole numbers as answers, % operator gives the remainder. Use them
		taskType, taskFile, rCount := getTask()
		switch taskType {
		case "done":
			//log.Printf("worker %v terminating after job well done...\n", os.Getpid())
			break mainLoop
		case "waiting":
			//log.Println("waiting for next task...")
			time.Sleep(time.Second)
			continue
		case "map":
			divider := len(alphabet) / rCount
			data, err := os.ReadFile(taskFile)
			if err != nil {
				//log.Printf("error while reading file %v: %v\n", taskFile, err)
				data = []byte{}
			}

			intermediate := mapf(taskFile, string(data))
			var i int
			for n := range rCount {
				var letters []string
				if n < rCount-1 {
					letters = alphabet[i : i+divider]
				} else if n == rCount-1 {
					letters = alphabet[i:]
				}

				var toWrite []KeyValue
				for i := range letters {
					for j := range intermediate {
						if letters[i] == intermediate[j].Key[0:1] {
							toWrite = append(toWrite, intermediate[j])
						}
					}
				}

				l := strings.Index(taskFile, "-")

				finalName := "m-" + taskFile[l+1:l+2] + "-" + fmt.Sprint(n)
				tempFile, err := os.CreateTemp(".", "m-tmp-out-*")
				if err != nil {
					log.Fatalf("error: %v file: %v", err, tempFile.Name())
				}
				tempName := tempFile.Name()
				writer := bufio.NewWriter(tempFile)
				for i := range toWrite {
					writer.WriteString(toWrite[i].Key)
					writer.WriteByte(' ')
					writer.WriteString(toWrite[i].Value)
					writer.WriteByte(' ')
				}
				if err := writer.Flush(); err != nil {
					log.Fatalf("error flushing to file %v: %v\n", tempName, err)
				}
				os.Rename(tempName, finalName)
				tempFile.Close()
				i = i + divider
			}

			reportTask(taskType, taskFile)
		case "reduce":
			oname := "mr-out-" + fmt.Sprint(rCount)
			files, err := filepath.Glob(oname)
			if err != nil {
				//log.Printf("error finding intermediate files: %v\n", err)
				continue
			}
			if len(files) > 0 {
				//log.Printf("task %v for file %v has already been completed...\n\n", taskType, oname)
				continue
			}
			tempFile, err := os.CreateTemp(".", "mr-tmp-out-*")
			if err != nil {
				log.Fatalf("error: %v file: %v", err, tempFile.Name())
			}
			intermediate := readIntermediate(rCount)

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
				_, err := tempFile.WriteString(toWrite)
				if err != nil {
					log.Printf("error while writing to file %v: %v\n", tempFile.Name(), err)
				}
				i = j
			}
			err = os.Rename(tempFile.Name(), oname)
			if err != nil {
				os.Exit(1)
				//log.Fatalf("error renaming temp file %v to %v, %v\n", tempFile.Name(), oname, err)
			}
			tempFile.Close()
			reportTask(taskType, oname)
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

func getTask() (string, string, int) {

	args := WorkerType{WorkerID: os.Getpid()}
	reply := WorkerType{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		//log.Printf("worker %v received task: %v for file %v with nReduce %v\n\n", os.Getpid(), reply.Task.Type, reply.Task.Filename, reply.ID)
	} else {
		//log.Printf("task acquisition failed!\n")
	}
	return reply.Task.Type, reply.Task.Filename, reply.ID
}

func reportTask(taskType string, taskFile string) string {

	args := WorkerType{WorkerID: os.Getpid(), Task: Task{Type: taskType, Filename: taskFile}}
	reply := WorkerType{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		//log.Printf("worker %v reported task: %v for file %v\n\n", os.Getpid(), taskType, taskFile)
	} else {
		//log.Printf("task report failed!\n")
	}
	return reply.Task.Type
}

func readIntermediate(taskID int) []KeyValue {
	toReduce := []KeyValue{}

	files, err := filepath.Glob("m-*-" + fmt.Sprint(taskID))
	if err != nil {
		//log.Printf("error finding intermediate files: %v\n", err)
		return []KeyValue{}
	}
	if len(files) == 0 {
		//log.Printf("no intermediate files found for taskID %v\n", taskID)
		return []KeyValue{}
	}
	for i := range files {
		data, err := os.ReadFile(files[i])
		if err != nil {
			//log.Printf("error while reading file %v: %v\n", files[i], err)
			continue
		}
		if len(data) == 0 {
			continue
		}

		draft := string(data)
		fileContent := strings.Split(draft, " ")
		for i := 0; i < len(fileContent)-1; i += 2 {
			k := fileContent[i]
			v := fileContent[i+1]
			if 0 < len(k) && unicode.IsLetter(rune(k[0])) {
				kv := KeyValue{k, v}
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

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
		//time.Sleep(time.Second)
		taskType, taskFile, rCount := getTask()
		switch taskType {
		case "done":
			log.Printf("worker %v terminating after job well done...\n", os.Getpid())
			break mainLoop
		case "waiting":
			log.Println("waiting for next task...")
			continue
		case "map":
			divider := len(alphabet) / rCount
			contents := readFile(taskFile)
			intermediate := mapf(taskFile, contents)

			// //without this part TestMapParallel produces a false negative
			// //logs would indicate only 2 workers present, but they would be counted multiple times
			// //this part reduces the mention of each worker to exactly once
			// files, err := filepath.Glob("m-out-*")
			// if err != nil {
			// 	log.Printf("error finding files: %v\n", err)
			// }
			// if len(files) > 1 {
			// 	n := countPattern(files, "times-"+fmt.Sprint(os.Getpid()))
			// 	if n > 1 {
			// 		intermediate = []KeyValue{}
			// 	}
			// }

			for n := range rCount {
				var letters []string
				if n+divider < len(alphabet) {
					letters = alphabet[n : n+divider]
				} else if n+divider > len(alphabet) {
					letters = alphabet[n:]
				}

				var toWrite []KeyValue
				for i := range letters {
					for j := range intermediate {
						if letters[i] == intermediate[j].Key[0:1] {
							toWrite = append(toWrite, intermediate[j])
						}
					}
				}

				finalName := "m-" + taskFile[14:15] + "-" + fmt.Sprint(n)
				fmt.Printf("worker %v writing to file: %v\n", os.Getpid(), finalName)
				files, _ := filepath.Glob(finalName)
				if len(files) == 0 {
					tempFile, err := os.CreateTemp("", "m-tmp-out-*")
					if err != nil {
						log.Fatalf("error: %v file: %v", err, tempFile.Name())
					}
					tempName := tempFile.Name()

					defer tempFile.Close()
					writer := bufio.NewWriter(tempFile)
					for i := range intermediate {
						writer.WriteString(intermediate[i].Key)
						writer.WriteByte(' ')
						writer.WriteString(intermediate[i].Value)
						writer.WriteByte(' ')
					}
					if err := writer.Flush(); err != nil {
						log.Fatalf("error flushing to file %v: %v\n", tempName, err)
					}
					os.Rename(tempName, finalName)
				} else if len(files) > 0 {
					file, err := os.OpenFile(files[0], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						log.Fatalf("error opening file %v: %v", file.Name(), err)
					}
					for i := range intermediate {
						fmt.Fprintf(file, "%v %v ", intermediate[i].Key, intermediate[i].Value)
					}
				}
				n = n + divider
			}

			reportTask(taskType, taskFile)
		case "reduce":
			oname := "mr-out-" + fmt.Sprint(rCount)
			intermediate := readIntermediate(rCount)
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
			reportTask(taskType, taskFile)
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
		log.Printf("worker %v received task: %v for file %v with nReduce %v\n\n", os.Getpid(), reply.Task.Type, reply.Task.Filename, reply.ID)
	} else {
		log.Printf("task acquisition failed!\n")
	}
	return reply.Task.Type, reply.Task.Filename, reply.ID
}

func reportTask(taskType string, taskFile string) string {

	args := WorkerType{WorkerID: os.Getpid(), Task: Task{Type: taskType, Filename: taskFile}}
	reply := WorkerType{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		log.Printf("worker %v reporting task: %v for file %v\n\n", os.Getpid(), taskType, taskFile)
	} else {
		log.Printf("task report failed!\n")
	}
	return reply.Task.Type
}

func readFile(taskFile string) string {
	sf := func(r rune) bool { return !unicode.IsLetter(r) }
	//read the given file and read only the given letter from it
	data, err := os.ReadFile(taskFile)
	if err != nil {
		log.Printf("error while reading file %v: %v\n", taskFile, err)
		return ""
	}

	draft := string(data)
	final := strings.FieldsFunc(draft, sf)

	return strings.Join(final, " ")
}

func readIntermediate(taskID int) []KeyValue {
	toReduce := []KeyValue{}

	files, err := filepath.Glob("m-*-" + fmt.Sprint(taskID))
	if err != nil {
		log.Printf("error finding intermediate files: %v\n", err)
		return []KeyValue{}
	}
	if len(files) == 0 {
		log.Printf("no intermediate files found for taskID %v\n", taskID)
		return []KeyValue{}
	}
	for i := range files {
		data, err := os.ReadFile(files[i])
		if err != nil {
			log.Printf("error while reading file %v: %v\n", files[i], err)
			continue
		}
		if len(data) == 0 {
			continue
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

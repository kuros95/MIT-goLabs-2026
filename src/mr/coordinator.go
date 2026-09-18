package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
	"time"
)

var alphabet = []string{"A", "a", "B", "b", "C", "c", "D", "d", "E", "e", "F", "f", "G", "g",
	"H", "h", "I", "i", "J", "j", "K", "k", "L", "l", "M", "m", "N", "n", "O", "o", "P", "p",
	"Q", "q", "R", "r", "S", "s", "T", "t", "U", "u", "V", "v", "W", "w", "X", "x", "Y", "y",
	"Z", "z"}

type mappedFile struct {
	name    string
	letters []string
}

type Coordinator struct {
	// Your definitions here.
	isDone        bool
	filesToMap    []string
	mappedFiles   []mappedFile
	filesToReduce []string
	workers       []WorkerType
	// You can use channels, mutexes, or other synchronization primitives to manage task assignment and completion.
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *WorkerType, reply *WorkerType) error {
	// TaskID is the letter for which worker is generating intermediate data.
	found := false
	for w := range c.workers {
		if c.workers[w].WorkerID == args.WorkerID {
			found = true
			break
		}
	}
	if !found {
		c.workers = append(c.workers, *args)
		fmt.Printf("found a new worker! %v\n", args.WorkerID)
	}
	if found {
		fmt.Printf("giving another task to worker %v\n", args.WorkerID)
	}

	if len(c.filesToReduce) > 0 {
		reply.Task.TaskType = "reduce"
		reply.Task.TaskID = c.filesToReduce[len(c.filesToReduce)-1][6:7]
		reply.Task.Filename = c.filesToReduce[len(c.filesToReduce)-1]

	} else if len(c.filesToReduce) == 0 {
		reply.Task.TaskType = "map"
		reply.Task.Filename = c.filesToMap[0]

		for _, f := range c.mappedFiles {
			if f.name == reply.Task.Filename && len(f.letters) < len(alphabet) {
				reply.Task.TaskID = alphabet[len(f.letters)]
			}
		}

	} else if len(c.filesToMap) == 0 && len(c.filesToReduce) == 0 {
		reply.Task.TaskType = "done"
	}
	reply.TimeStamp = time.Now()
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task = reply.Task
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].TimeStamp = reply.TimeStamp
	fmt.Printf("worker status: %v\n", c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })])
	fmt.Printf("worker %v has been given task: type: %v, file: %v, letter: %v at %v\n", args.WorkerID, reply.Task.TaskType, reply.Task.Filename, reply.Task.TaskID, reply.TimeStamp)
	return nil
}

func (c *Coordinator) ReportTask(args *WorkerType, reply *WorkerType) error {
	fmt.Printf("reporting task %v from worker %v for file %v and letter %v\n", args.Task.TaskType, args.WorkerID, args.Task.Filename, args.Task.TaskID)
	if c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].IsReassigned == true {
		reply.Task.TaskType = "waiting"
		c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task.TaskType = reply.Task.TaskType
		fmt.Printf("task %v for file %v and letter %v has already been reassigned...\n", args.Task.TaskType, args.Task.Filename, args.Task.TaskID)
		return nil
	}

	fmt.Printf("status of files to map: %v\n", c.filesToMap)
	fmt.Printf("status of mapped files and letters: %v\n", c.mappedFiles)
	fmt.Printf("status of files to reduce: %v\n", c.filesToReduce)
	fmt.Printf("will now check for file %v...\n", args.Task.Filename[8:])
	switch args.Task.TaskType {
	case "map":
		fmt.Println("checking for file in mappedFiles...")
		for i, m := range c.mappedFiles {
			fmt.Printf("checking if %v is equal to %v\n", args.Task.Filename[8:], m.name)
			if args.Task.Filename[8:] == m.name && len(m.letters) < len(alphabet) {
				fmt.Println("found!")
				c.filesToReduce = append(c.filesToReduce, args.Task.Filename)
				c.mappedFiles[i].letters = append(c.mappedFiles[i].letters, args.Task.TaskID)
			} else if args.Task.Filename[8:] == m.name && len(m.letters) == len(alphabet) {
				fmt.Println("found!")
				c.filesToMap = slices.Delete(c.filesToMap, 0, 1)
				c.filesToReduce = append(c.filesToReduce, args.Task.Filename)
				c.mappedFiles[i].letters = append(c.mappedFiles[i].letters, args.Task.TaskID)
			}
		}

	case "reduce":
		index := slices.Index(c.filesToReduce, args.Task.Filename)
		fmt.Printf("file %v is at index %v\n", args.Task.Filename, index)
		c.filesToReduce = slices.Delete(c.filesToReduce, index, index+1)
	}

	reply.Task.TaskType = "waiting"
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task.TaskType = reply.Task.TaskType
	fmt.Printf("task %v for file %v has been reported; setting worker %v to waiting...\n\n", args.Task.TaskType, args.Task.Filename, args.WorkerID)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v\n", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if len(c.filesToMap) == 0 && len(c.filesToReduce) == 0 {
		ret = true
	}

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	m := sync.Mutex{}
	c.filesToMap = files
	c.mappedFiles = func() []mappedFile {
		for _, f := range files {
			c.mappedFiles = append(c.mappedFiles, mappedFile{f, []string{}})
		}
		return c.mappedFiles
	}()
	fmt.Printf("coordinator working with files: %v\n", files)

	// Your code here.
	// TODO: Add info prints

	c.server(sockname)
	fmt.Printf("Coordinator is listening on %v, waiting for workers to connect...\n", sockname)
	go func(m *sync.Mutex) {
		for {
			for w := range c.workers {
				if time.Since(c.workers[w].TimeStamp) > time.Duration(10*time.Second) {
					m.Lock()
					c.workers[w].IsReassigned = true
					m.Unlock()
				}
			}
		}
	}(&m)
	return &c
}

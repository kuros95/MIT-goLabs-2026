package mr

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

var alphabet = []string{"A", "a", "B", "b", "C", "c", "D", "d", "E", "e", "F", "f", "G", "g",
	"H", "h", "I", "i", "J", "j", "K", "k", "L", "l", "M", "m", "N", "n", "O", "o", "P", "p",
	"Q", "q", "R", "r", "S", "s", "T", "t", "U", "u", "V", "v", "W", "w", "X", "x", "Y", "y",
	"Z", "z"}

var workFiles = []string{}

type mappedLetter struct {
	letter string
	names  []string
}

type Coordinator struct {
	// Your definitions here.
	isDone        bool
	filesToMap    []string
	lettersToMap  []string
	mappedLetters []mappedLetter
	filesToReduce []string
	workers       []WorkerType
	mutex         sync.Mutex
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

	if len(c.lettersToMap) == 0 && len(c.filesToReduce) == 0 {
		reply.Task.TaskType = "done"

	} else if len(c.filesToReduce) > 0 {
		reply.Task.TaskType = "reduce"
		reply.Task.TaskID = c.filesToReduce[len(c.filesToReduce)-1][6:7]
		reply.Task.Filename = c.filesToReduce[len(c.filesToReduce)-1]

	} else if len(c.filesToReduce) == 0 {
		reply.Task.TaskID = c.lettersToMap[0]
		for _, l := range c.mappedLetters {
			if l.letter == reply.Task.TaskID && len(l.names) == len(workFiles) {
				reply.Task.TaskType = "waiting"
				continue
			} else if l.letter == reply.Task.TaskID && len(l.names) < len(workFiles) {
				reply.Task.TaskType = "map"
				reply.Task.Filename = workFiles[len(l.names)]
			}
		}

	}

	reply.TimeStamp = time.Now()
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task = reply.Task
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].TimeStamp = reply.TimeStamp
	fmt.Printf("worker %v has been given task: type: %v, file: %v, letter: %v at %v\n\n", args.WorkerID, reply.Task.TaskType, reply.Task.Filename, reply.Task.TaskID, reply.TimeStamp)
	return nil
}

func (c *Coordinator) ReportTask(args *WorkerType, reply *WorkerType) error {
	fmt.Printf("reporting task %v from worker %v for file %v and letter %v\n", args.Task.TaskType, args.WorkerID, args.Task.Filename, args.Task.TaskID)
	if c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].IsReassigned == true {
		reply.Task.TaskType = "waiting"
		c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task.TaskType = reply.Task.TaskType
		fmt.Printf("task %v for file %v and letter %v has already been reassigned...\n\n", args.Task.TaskType, args.Task.Filename, args.Task.TaskID)
		return nil
	}

	switch args.Task.TaskType {
	case "map":
		for i, m := range c.mappedLetters {
			if args.Task.TaskID == m.letter && len(m.names) < len(workFiles) {
				c.mutex.Lock()
				c.mappedLetters[i].names = append(c.mappedLetters[i].names, args.Task.Filename[8:])
				c.mutex.Unlock()
				if len(c.mappedLetters[i].names) == len(workFiles) {
					c.mutex.Lock()
					c.lettersToMap = slices.Delete(c.lettersToMap, 0, 1)
					c.filesToReduce = append(c.filesToReduce, args.Task.Filename)
					c.mutex.Unlock()
				}
			}
		}

	case "reduce":
		c.mutex.Lock()
		index := slices.Index(c.filesToReduce, args.Task.Filename)
		c.filesToReduce = slices.Delete(c.filesToReduce, index, index+1)
		c.mutex.Unlock()
	}

	reply.Task.TaskType = "waiting"
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task.TaskType = reply.Task.TaskType
	fmt.Printf("task %v for file %v has been reported; setting worker %v to waiting...\n\n", args.Task.TaskType, args.Task.Filename, args.WorkerID)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v\n", sockname, e)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Printf("connection accepting error: %v\n", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if len(c.lettersToMap) == 0 && len(c.filesToReduce) == 0 {
		fmt.Println("removing intermediate files...")
		files, err := filepath.Glob("m-out-*")
		if err != nil {
			fmt.Printf("error finding intermediate files: %v\n", err)
		}
		for _, f := range files {
			os.Remove(f)
		}

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
	//slices.Sort(alphabet)
	c.filesToMap = files
	c.lettersToMap = alphabet
	workFiles = files
	c.mappedLetters = func() []mappedLetter {
		for _, l := range alphabet {
			c.mappedLetters = append(c.mappedLetters, mappedLetter{l, []string{}})
		}
		return c.mappedLetters
	}()
	fmt.Printf("coordinator working with files: %v\n", files)

	// Your code here.

	c.server(sockname)
	fmt.Printf("coordinator is listening on %v, waiting for workers to connect...\n", sockname)
	go func() {
		for {
			for w := range c.workers {
				if time.Since(c.workers[w].TimeStamp) > time.Duration(10*time.Second) {
					c.mutex.Lock()
					c.workers[w].IsReassigned = true
					c.mutex.Unlock()
				}
			}
		}
	}()
	return &c
}

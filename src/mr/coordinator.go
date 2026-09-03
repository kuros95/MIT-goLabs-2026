package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
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
	assignedIDs   []string
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
	//
	found := false
	for w := range c.workers {
		if c.workers[w].WorkerID == args.WorkerID {
			found = true
			break
		}
	}
	if !found {
		c.workers = append(c.workers, *args)
	}

	if len(c.filesToReduce) > 0 {
		reply.Task.TaskType = "reduce"
		reply.Task.TaskID = c.filesToReduce[len(c.filesToReduce)-1][6:7]
		reply.Task.Filename = c.filesToReduce[len(c.filesToReduce)-1]

	} else if len(c.filesToReduce) == 0 {
		reply.Task.TaskType = "map"
		reply.Task.Filename = c.filesToMap[0]

		for _, f := range c.mappedFiles {
			if f.name == c.filesToMap[0] && len(f.letters) < len(alphabet) {
				reply.Task.TaskID = alphabet[len(f.letters)]
			}
		}

	} else if len(c.filesToMap) == 0 && len(c.filesToReduce) == 0 {
		reply.Task.TaskType = "done"
	}
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task = reply.Task

	return nil
}

func (c *Coordinator) ReportTask(args *WorkerType, reply *WorkerType) error {
	// Set task status to waiting, so that worker ID is preserved.
	if args.Task.TaskType == "map" {
		c.filesToMap = slices.Delete(c.filesToMap, args.Task.TaskID, args.Task.TaskID+1)
		c.filesToReduce = append(c.filesToReduce, args.Task.Filename)
	} else if args.Task.TaskType == "reduce" {
		c.filesToReduce = slices.Delete(c.filesToReduce, args.Task.TaskID, args.Task.TaskID+1)
	}

	for w := range c.workers {
		if c.workers[w].Task.TaskID == args.Task.TaskID {
			c.workers[w].Task.TaskType = "waiting"
		}
	}
	reply.Task.TaskType = "waiting"

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
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
	c.filesToMap = files
	c.mappedFiles = func() []mappedFile {
		for _, f := range files {
			c.mappedFiles = append(c.mappedFiles, mappedFile{f, []string{}})
		}
		return c.mappedFiles
	}()

	// after mapping one letter throughout all files, combine into one []KeyValue
	// and write to file to send to Reduce worker.

	// Your code here.

	c.server(sockname)
	return &c
}

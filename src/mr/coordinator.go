package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
)

type Coordinator struct {
	// Your definitions here.
	isDone        bool
	filesToMap    []string
	filesToReduce []string
	tasks         []Task
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

func (c *Coordinator) GetTask(args *WorkerType, reply *Task) error {

	// Assign the first file in the list for mapping
	// Assign file index as task ID
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
		reply.TaskType = "reduce"
		reply.TaskID = len(c.filesToReduce) - 1
		reply.Filename = c.filesToReduce[reply.TaskID]
	} else if len(c.filesToReduce) == 0 {
		reply.TaskType = "map"
		isProcessed := false
		for f := range c.filesToMap {
			for w := range c.workers {
				if c.workers[w].Task.Filename == c.filesToMap[f] {
					isProcessed = true
					break
				}
			}
			if !isProcessed {
				reply.TaskID = slices.Index(c.filesToMap, c.filesToMap[f])
			}
		}
		reply.Filename = c.filesToMap[reply.TaskID]
	} else if len(c.filesToMap) == 0 && len(c.filesToReduce) == 0 {
		reply.TaskType = "done"
	}
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task = *reply
	c.tasks = append(c.tasks, *reply)

	return nil
}

func (c *Coordinator) ReportTask(args *Task, reply *Task) error {
	// Set task status to waiting, so that worker ID is preserved.
	if args.TaskType == "map" {
		c.filesToMap = slices.Delete(c.filesToMap, args.TaskID, args.TaskID+1)
		c.filesToReduce = append(c.filesToReduce, args.Filename)
	} else if args.TaskType == "reduce" {
		c.filesToReduce = slices.Delete(c.filesToReduce, args.TaskID, args.TaskID+1)
	}

	reply.TaskType = "waiting"

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

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server(sockname)
	return &c
}

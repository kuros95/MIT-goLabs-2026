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

// TODO: Set of letters to be decided by the noReduce.
// TODO: Each Map round will consist of a number of letters and all files and produce only ONE m-out-* file.
// TODO: Schedule mapping of all files with a given set of letters.

type Coordinator struct {
	// Your definitions here.
	filesToMap   []string
	filesReduced []string
	workers      []WorkerType
	rCount       int
	noReduce     int
	mutex        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) updateWorkers(ID int, task Task) {
	n := slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == ID })
	if n != -1 {
		c.workers[n].Task = task
		c.workers[n].TimeStamp = time.Now()
		c.workers[n].IsReassigned = false
	}
}

func (c *Coordinator) GetTask(args *WorkerType, reply *WorkerType) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	found := false
	for w := range c.workers {
		if c.workers[w].WorkerID == args.WorkerID {
			found = true
			break
		}
	}
	if !found {
		c.workers = append(c.workers, *args)
		log.Printf("found a new worker! %v\n", args.WorkerID)

	}
	if found {
		log.Printf("giving another task to worker %v\n", args.WorkerID)
	}

	fmt.Printf("rCount: %v\n", c.rCount)
	fmt.Printf("noReduce: %v\n", c.noReduce)

	if c.rCount == c.noReduce {
		reply.Task.Type = "done"
		c.updateWorkers(args.WorkerID, reply.Task)
		return nil

	} else if len(c.filesToMap) == 0 {
		reply.Task.Type = "reduce"
		reply.ID = c.noReduce
		c.updateWorkers(args.WorkerID, reply.Task)
		log.Printf("worker %v has been given task: %v, file: %v at %v\n\n", args.WorkerID, reply.Task.Type, reply.Task.Filename, reply.TimeStamp.Format(time.DateTime))
		return nil

	} else if len(c.filesToMap) > 0 {
		// The / operator gives whole numbers as answers, % operator gives the remainder. Use them
		reply.Task.Filename = c.filesToMap[0]
		reply.ID = c.rCount
		reply.Task.Type = "map"
		c.updateWorkers(args.WorkerID, reply.Task)
		log.Printf("worker %v has been given task: %v, file: %v at %v\n\n", args.WorkerID, reply.Task.Type, reply.Task.Filename, reply.TimeStamp.Format(time.DateTime))
		return nil
	}

	reply.Task.Type = "waiting"
	log.Printf("worker %v has been given task: %v, file: %v at %v\n\n", args.WorkerID, reply.Task.Type, reply.Task.Filename, reply.TimeStamp.Format(time.DateTime))
	return nil
}

func (c *Coordinator) ReportTask(args *WorkerType, reply *WorkerType) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	log.Printf("reporting task %v from worker %v for file %v\n", args.Task.Type, args.WorkerID, args.Task.Filename)
	if c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].IsReassigned == true {
		reply.Task.Type = "waiting"
		c.updateWorkers(args.WorkerID, reply.Task)
		log.Printf("task %v for file %v has already been reassigned...\n\n", args.Task.Type, args.Task.Filename)
		return nil
	}

	if args.Task.Type == "reduce" {
		if slices.IndexFunc(c.filesReduced, func(f string) bool { return f == args.Task.Filename }) != -1 {
			reply.Task.Type = "waiting"
			c.updateWorkers(args.WorkerID, reply.Task)
			log.Printf("task %v for file %v has already been completed...\n\n", args.Task.Type, args.Task.Filename)
			return nil
		}
		c.filesReduced = append(c.filesReduced, args.Task.Filename)
		if c.noReduce < c.rCount {
			c.noReduce++
		}
	}

	if args.Task.Type == "map" {
		index := slices.IndexFunc(c.filesToMap, func(f string) bool { return f == args.Task.Filename })
		if index == -1 {
			log.Printf("task %v for file %v has already been completed...\n\n", args.Task.Type, args.Task.Filename)
			reply.Task.Type = "waiting"
			c.updateWorkers(args.WorkerID, reply.Task)
			return nil
		}
		c.filesToMap = slices.Delete(c.filesToMap, index, index+1)

	}
	reply.Task.Type = "waiting"
	c.updateWorkers(args.WorkerID, reply.Task)
	log.Printf("task %v for file %v has been reported; setting worker %v to waiting...\n\n", args.Task.Type, args.Task.Filename, args.WorkerID)
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
				log.Printf("connection accepting error: %v\n", err)
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

	c.mutex.Lock()
	isDone := c.rCount == c.noReduce
	c.mutex.Unlock()
	if isDone {
		log.Println("removing intermediate files...")
		files, err := filepath.Glob("m-*")
		if err != nil {
			log.Printf("error finding intermediate files: %v\n", err)
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
	//Consider a reader to get all possible first letters and become independent from standard alphabet
	//REBUILD: In order to pass tests the map phase has to be completed before any reduce work can begin.
	//Each file has to be mapped exactly once.
	//Output of map has to be distibuted between nReduce buckets.
	//When map is complete, reduce will work on said buckets in alphabetical order.
	//When reduce is completed, send done.
	c := Coordinator{}
	//slices.Sort(alphabet)
	c.filesToMap = files
	c.rCount = nReduce
	log.Printf("coordinator working with files: %v\n", files)
	log.Printf("amount of reduce tasks: %v\n", c.rCount)

	// Your code here.

	c.server(sockname)
	log.Printf("coordinator is listening on %v, waiting for workers to connect...\n", sockname)
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.mutex.Lock()
			if len(c.workers) > 0 {
				for w := range c.workers {
					if time.Since(c.workers[w].TimeStamp) > time.Duration(10*time.Second) && c.workers[w].IsReassigned == false {
						c.workers[w].IsReassigned = true
						// switch c.workers[w].Task.Type {
						// case "map":
						// 	var indexToDelete int
						// 	for l := range c.mappedLetters {
						// 		if c.mappedLetters[l] == c.workers[w].Task.Letters[0] {
						// 			indexToDelete = l
						// 		}
						// 	}
						// 	if len(c.mappedLetters) > 0 {
						// 		c.mappedLetters = slices.Delete(c.mappedLetters, indexToDelete, indexToDelete+len(c.workers[w].Task.Letters))
						// 	}
						// 	c.lettersToMap = append(c.lettersToMap, c.workers[w].Task.Letters...)
						// case "reduce":
						// 	c.filesToReduce = append(c.filesToReduce, fileToReduce{c.workers[w].Task.Filenames[0], c.workers[w].Task.ID})
						// }
					}
				}
			}
			c.mutex.Unlock()

		}
	}()
	return &c
}

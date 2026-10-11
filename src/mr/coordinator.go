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
type task struct {
	ID   int
	done bool
}

type Coordinator struct {
	// Your definitions here.
	filesToMap   []string
	filesReduced []string
	workers      []WorkerType
	rTasks       []task
	mCount       int
	rCount       int
	noMap        int
	noReduce     int
	done         bool
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

	mrFiles, err := filepath.Glob("mr-out-*")
	if err != nil {
		log.Printf("error finding files: %v\n", err)
	}
	if len(mrFiles) > 0 {
		for i := range c.rTasks {
			slices.Sort(mrFiles)
			if !slices.Contains(mrFiles, "mr-out-"+fmt.Sprint(i)) {
				c.rTasks[i].done = false
				log.Printf("file mr-out-%v is missing; setting task %v to not done...\n", i, i)
			}
		}
	}
	fmt.Printf("rFiles: %v\n", mrFiles)

	fmt.Printf("mCount: %v\n", c.mCount)
	fmt.Printf("rCount: %v\n", c.rCount)
	fmt.Printf("noReduce: %v\n", c.noReduce)
	fmt.Printf("noMap: %v\n", c.noMap)
	fmt.Printf("rTasks: %v\n", c.rTasks)
	fmt.Printf("files reduced: %v\n", c.filesReduced)

	if c.noMap == c.mCount && c.noReduce == c.rCount {
		c.done = true
		reply.Task.Type = "done"
		c.updateWorkers(args.WorkerID, reply.Task)
		return nil

	} else if len(c.filesToMap) == 0 && c.noMap == c.mCount {

		newID := slices.IndexFunc(c.rTasks, func(t task) bool { return !t.done })
		if newID == -1 {
			reply.Task.Type = "waiting"
			c.updateWorkers(args.WorkerID, reply.Task)
			log.Printf("all reduce tasks are done/worked on; worker %v has been given task: %v...\n\n", args.WorkerID, reply.Task.Type)
			return nil
		}
		reply.ID = newID
		reply.Task.Filename = "mr-out-" + fmt.Sprint(newID)
		reply.Task.Type = "reduce"
		c.rTasks[newID].done = true
		c.filesReduced = append(c.filesReduced, "mr-out-"+fmt.Sprint(newID))
		c.updateWorkers(args.WorkerID, reply.Task)
		log.Printf("worker %v has been given task: %v, file: %v\n\n", args.WorkerID, reply.Task.Type, reply.Task.Filename)
		return nil

	} else if len(c.filesToMap) > 0 {
		// The / operator gives whole numbers as answers, % operator gives the remainder. Use them
		reply.Task.Filename = c.filesToMap[0]
		reply.Task.Type = "map"
		reply.ID = c.rCount
		c.filesToMap = slices.Delete(c.filesToMap, 0, 1)
		c.updateWorkers(args.WorkerID, reply.Task)
		log.Printf("worker %v has been given task: %v, file: %v\n\n", args.WorkerID, reply.Task.Type, reply.Task.Filename)
		return nil
	}

	reply.Task.Type = "waiting"
	log.Printf("worker %v has been given task: %v...\n\n", args.WorkerID, reply.Task.Type)
	return nil
}

func (c *Coordinator) ReportTask(args *WorkerType, reply *WorkerType) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	log.Printf("reporting task %v from worker %v for file %v\n", args.Task.Type, args.WorkerID, "mr-out-"+fmt.Sprint(args.ID))
	if args.Task.Type == "map" && c.noMap < c.mCount {
		c.noMap++
	}
	if args.Task.Type == "reduce" && c.noReduce < c.rCount {
		c.noReduce++
	}
	fmt.Printf("noMap: %v\n", c.noMap)
	fmt.Printf("mCount: %v\n", c.mCount)
	fmt.Printf("noReduce: %v\n", c.noReduce)
	fmt.Printf("rCount: %v\n", c.rCount)

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
	isDone := c.done
	c.mutex.Unlock()
	if isDone {
		log.Println("removing intermediate files...")
		files, err := filepath.Glob("m-*")
		if err != nil {
			log.Printf("error finding intermediate files: %v\n", err)
		}
		fmt.Printf("removing files: %v\n", files)
		//for _, f := range files {
		//	os.Remove(f)
		//}
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
	c.mCount = len(files)
	c.rCount = nReduce
	c.rTasks = make([]task, nReduce)
	for i := range nReduce {
		c.rTasks[i] = task{ID: i, done: false}
	}
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
						switch c.workers[w].Task.Type {
						case "map":
							c.filesToMap = append(c.filesToMap, c.workers[w].Task.Filename)
							log.Printf("worker %v has timed out and task has been reassigned: %v, file: %v\n\n", c.workers[w].WorkerID, c.workers[w].Task.Type, c.workers[w].Task.Filename)
						case "reduce":
							log.Printf("worker %v has timed out and task has been reassigned: %v, file: %v\n\n", c.workers[w].WorkerID, c.workers[w].Task.Type, "mr-out-"+fmt.Sprint(c.workers[w].ID))
							index := slices.IndexFunc(c.filesReduced, func(f string) bool { return f == "mr-out-"+fmt.Sprint(c.workers[w].ID) })
							fmt.Printf("index of file to be removed: %v, length: %v\n", index, len(c.filesReduced))
							c.rTasks[c.workers[w].ID].done = false
							if index != -1 {
								c.filesReduced = slices.Delete(c.filesReduced, index, index+1)
							}
							err := os.Remove("mr-out-" + fmt.Sprint(c.workers[w].ID))
							if err != nil {
								log.Printf("error removing file: %v\n", err)
							}
							if err == nil {
								log.Printf("file mr-out-%v has been removed\n", c.workers[w].ID)
							}
						}
					}
				}
			}
			c.mutex.Unlock()
		}
	}()
	return &c
}

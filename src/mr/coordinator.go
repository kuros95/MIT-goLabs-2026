package mr

import (
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

var divider, remainder, noOfReduce int

// TODO: Set of letters to be decided by the noReduce.
// TODO: Each Map round will consist of a number of letters and all files and produce only ONE m-out-* file.
// TODO: Schedule mapping of all files with a given set of letters.

type Coordinator struct {
	// Your definitions here.
	filesToMap    []string
	lettersToMap  []string
	mappedLetters []string
	filesToReduce []string
	workers       []WorkerType
	mCount        int
	rCount        int
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

	if len(c.mappedLetters) == len(alphabet) && len(c.filesToReduce) == 0 {
		reply.Task.Type = "done"

	} else if len(c.filesToReduce) > 0 {
		reply.Task.ID = c.rCount
		filename := []string{c.filesToReduce[0]}
		reply.Task.Filenames = append(reply.Task.Filenames, filename...)

		if index := slices.Index(c.filesToReduce, reply.Task.Filenames[0]); index != -1 {
			c.filesToReduce = slices.Delete(c.filesToReduce, index, index+1)
		}
		c.rCount++
		reply.Task.Type = "reduce"

	} else if len(c.filesToReduce) == 0 {
		// The / operator gives whole numbers as answers, % operator gives the remainder. Use them
		var chosenLetters []string
		reply.Task.ID = c.mCount

		if c.rCount < noOfReduce-2 {
			chosenLetters = c.lettersToMap[:divider]
		} else if c.rCount == noOfReduce-2 {
			chosenLetters = c.lettersToMap
		}
		reply.Task.Letters = append(reply.Task.Letters, chosenLetters...)

		reply.Task.Filenames = c.filesToMap

		c.mCount++
		reply.Task.Type = "map"
		if len(c.lettersToMap) > 0 {
			c.lettersToMap = slices.Delete(c.lettersToMap, 0, len(reply.Task.Letters))
		} else if len(c.lettersToMap) == 0 {
			reply.Task.Type = "waiting"
		}
	}

	reply.TimeStamp = time.Now()
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task = reply.Task
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].TimeStamp = reply.TimeStamp
	log.Printf("worker %v has been given task: type: %v, ID: %v, letters: %v at %v\n\n", args.WorkerID, reply.Task.Type, reply.Task.ID, reply.Task.Letters, reply.TimeStamp.Format(time.DateTime))
	return nil
}

func (c *Coordinator) ReportTask(args *WorkerType, reply *WorkerType) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	log.Printf("reporting task %v from worker %v for letters %v and ID %v\n", args.Task.Type, args.WorkerID, args.Task.Letters, args.Task.ID)
	if c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].IsReassigned == true {
		reply.Task.Type = "waiting"
		c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task.Type = reply.Task.Type
		log.Printf("task %v for letters %v and ID %v has already been reassigned...\n\n", args.Task.Type, args.Task.Letters, args.Task.ID)
		return nil
	}

	if args.Task.Type == "map" {
		c.mappedLetters = append(c.mappedLetters, args.Task.Letters...)
		c.filesToReduce = append(c.filesToReduce, args.Task.Filenames[0])
	}

	reply.Task.Type = "waiting"
	c.workers[slices.IndexFunc(c.workers, func(w WorkerType) bool { return w.WorkerID == args.WorkerID })].Task.Type = reply.Task.Type
	log.Printf("task %v for letters %v in file %v has been reported; setting worker %v to waiting...\n\n", args.Task.Type, args.Task.Letters, args.Task.Filenames[0], args.WorkerID)
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
	isDone := len(c.mappedLetters) == len(alphabet) && len(c.filesToReduce) == 0
	c.mutex.Unlock()
	if isDone {
		log.Println("removing intermediate files...")
		files, err := filepath.Glob("m-out-*")
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
	c := Coordinator{}
	//slices.Sort(alphabet)
	c.filesToMap = files
	c.lettersToMap = alphabet
	workFiles = files
	noOfReduce = nReduce
	divider = len(alphabet) / nReduce
	remainder = len(alphabet) % nReduce
	log.Printf("coordinator working with files: %v\n", files)

	// Your code here.

	c.server(sockname)
	log.Printf("coordinator is listening on %v, waiting for workers to connect...\n", sockname)
	go func() {
		for {
			c.mutex.Lock()
			workers := make([]WorkerType, len(c.workers))
			copy(workers, c.workers)
			c.mutex.Unlock()
			for w := range workers {
				c.mutex.Lock()
				timeSince := time.Since(c.workers[w].TimeStamp)
				c.mutex.Unlock()
				if timeSince > time.Duration(10*time.Second) {
					c.mutex.Lock()
					c.workers[w].IsReassigned = true
					switch c.workers[w].Task.Type {
					case "map":
						c.mCount--
						for l := range c.mappedLetters {
							if c.mappedLetters[l] == c.workers[w].Task.Letters[0] {
								c.mappedLetters = slices.Delete(c.mappedLetters, l, l+len(c.workers[w].Task.Letters))
							}
						}
					case "reduce":
						c.rCount--
						c.filesToReduce = append(c.filesToReduce, c.workers[w].Task.Filenames[0])
					}
					c.mutex.Unlock()
				}
			}
		}
	}()
	return &c
}

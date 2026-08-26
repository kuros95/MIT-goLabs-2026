package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Task struct {
	TaskID   int
	TaskType string // "map" or "reduce"
	Filename string // for map tasks, the input file name; for reduce tasks, can be empty or a list of intermediate files
}

type WorkerType struct {
	WorkerID  int
	IsWorking bool
	Task      Task
}

// Add your RPC definitions here.

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

type IsWorking struct {
	Status bool
}

type Task struct {
	TaskID    int
	TaskType  string // "map" or "reduce"
	Filename  string // for map tasks, the input file name; for reduce tasks, can be empty or a list of intermediate files
	IsWorking bool   // true if the worker is currently working on a task, false otherwise
}

// Add your RPC definitions here.

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
	TaskID   string
	TaskType string
	Filename string
}

type WorkerType struct {
	WorkerID  int
	IsWorking bool
	Task      Task
}

// Add your RPC definitions here.

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

type AmIWorking struct {
	Status bool
}

type GivenTask struct {
	TaskID   int
	TaskType string // "map" or "reduce"
	Filename string // for map tasks, the input file name; for reduce tasks, can be empty or a list of intermediate files
}

// Add your RPC definitions here.

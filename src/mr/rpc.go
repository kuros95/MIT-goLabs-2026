package mr

import "time"

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
	Type     string
	Filename string
}

type WorkerType struct {
	WorkerID     int
	IsReassigned bool
	Task         Task
	rCount       int
	TimeStamp    time.Time
}

// Add your RPC definitions here.

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type Arg struct {
}

type Report struct {
	Maptask   bool
	Taskindex int
	Info      string
}

type Assign struct {
	//回应时，存储分配的任务
	NReduce   int
	Task      bool
	Maptask   bool //true:map task flase: reduce task
	Taskindex int
	Info      string   //map file name
	RedFiles  []string // reduce file names
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

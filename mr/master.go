package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Master struct {
	// Your definitions here.
	//谁没被map
	Mu      sync.Mutex
	Mapmode bool //is it in mapmode?

	Maped    map[string]bool //文件名->是否maped
	Maping   map[string]int
	Maptime  map[int]int
	Mapindex int

	Reded    map[int]bool //X-reduce done?
	Reding   map[int]int  //who is running X-reduce
	Redindex int          //
	nReduce  int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Apply(args *Assign, reply *Assign) error {
	//m.Mu.Lock() //sametime allow only one thread apply
	if m.Mapphase() { //if it's in mapmode
		for fname := range m.Maping {
			if m.Maping[fname] < 0 {
				//assign this
				m.Maping[fname] = m.Mapindex
				reply.Task = true
				reply.Maptask = true
				reply.Taskindex = m.Mapindex
				reply.Info = fname
				reply.nReduce = m.nReduce
				m.Mapindex++
				return nil
			}
		}
	} else {
		//then it's in redmode
		//ill read the seq them modify the reduce pro
		reply.Task = true
		reply.Maptask = false
		reply.Taskindex = m.Redindex
		reply.Info = "Reduce Task" + strconv.Itoa(m.Redindex)
		m.Redindex++
	}

	return nil
}

func (m *Master) Mapphase() bool {
	var mapmode bool = false
	for fname := range m.Maped {
		if !m.Maped[fname] {
			mapmode = true
			break
		}
	}
	m.Mapmode = mapmode
	return mapmode
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	fmt.Println("长度", len(files))
	fmt.Println(files)

	//initiating maped&maping
	m.Maped = make(map[string]bool)
	m.Maping = make(map[string]int)
	for _, filename := range files {
		m.Maped[filename] = false
		m.Maping[filename] = -1
	}
	m.Mapindex = 0
	m.Redindex = 0
	m.nReduce = nReduce
	m.server()
	return &m
}

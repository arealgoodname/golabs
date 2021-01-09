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
	"time"
)

type Master struct {
	// Your definitions here.
	//谁没被map
	Mu      sync.Mutex
	Mapmode bool //is it in mapmode?

	Maped  map[string]bool //文件名->是否maped
	Maping map[string]int
	//Maptime  map[int]int
	Mapindex int

	Reded    map[int]bool //X-reduce done?
	Reding   map[int]int  //who is running X-reduce
	Redindex int          //
	NReduce  int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Apply(args *Assign, reply *Assign) error {
	m.Mu.Lock()       //sametime allow only one thread apply
	if m.Mapphase() { //if it's in mapmode
		for fname := range m.Maping {
			if m.Maping[fname] < 0 {
				//assign this
				m.Maping[fname] = m.Mapindex
				reply.Task = true
				reply.Maptask = true
				reply.Taskindex = m.Mapindex
				reply.Info = fname
				reply.NReduce = m.NReduce
				m.Mapindex++
				fmt.Println("assign maptask " + strconv.Itoa(reply.Taskindex))
				break
			}
		}
	} else {
		//then it's in redmode
		//ill read the seq them modify the reduce pro
		for iRed := range m.Reding {
			if m.Reding[iRed] < 0 {
				m.Reding[iRed] = m.Redindex
				reply.Task = true
				reply.Maptask = false
				reply.Taskindex = m.Redindex
				reply.NReduce = m.NReduce
				reply.Info = "Reduce Task" + strconv.Itoa(m.Redindex)
				for i := 0; i < len(m.Maped); i++ {
					reply.RedFiles = append(reply.RedFiles, "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(iRed)+".json")
				}
				m.Redindex++
				fmt.Println("assign redtask " + strconv.Itoa(reply.Taskindex))
				break
			}
		}
	}
	m.Mu.Unlock()
	return nil
}

func (m *Master) Jobdone(report *Report, infoBack *Report) error {
	if report.Maptask {
		fmt.Println("maptask" + strconv.Itoa(report.Taskindex) + "want the lock")
	} else {
		fmt.Println("redtask" + strconv.Itoa(report.Taskindex) + "want the lock")
	}
	//m.Mu.Lock()
	if report.Maptask {
		fmt.Println("maptask" + strconv.Itoa(report.Taskindex) + "got the lock")
	} else {
		fmt.Println("redtask" + strconv.Itoa(report.Taskindex) + "got the lock")
	}
	if report.Maptask {
		m.Maped[report.Info] = true
		fmt.Println("Map task " + strconv.Itoa(report.Taskindex) + "filename: " + report.Info + " done")
	} else {
		fmt.Println("update reduce task process")
	}
	if report.Maptask {
		fmt.Println("maptask" + strconv.Itoa(report.Taskindex) + "release the lock")
	} else {
		fmt.Println("redtask" + strconv.Itoa(report.Taskindex) + "release the lock")
	}
	//m.Mu.Unlock()
	return nil
}

func (m *Master) WaitCheck(task Assign) bool {
	//overtime test
	timer := time.NewTimer(time.Second * 10)

	<-timer.C
	if task.Maptask {
		if !m.Maped[task.Info] {
			//reverse assign
			m.Maping[task.Info] = -1
			return false
		}
	} else {
		if !m.Reded[task.Taskindex] {
			m.Reding[task.Taskindex] = -1
			return false
		}
	}
	return true
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

	m.NReduce = nReduce
	//fmt.Println("nimasile,makemaster nReduce = ", m.nReduce)
	m.Reding = make(map[int]int)
	m.Reded = make(map[int]bool)
	for i := 0; i < m.NReduce; i++ {
		m.Reding[i] = -1
		m.Reded[i] = false
	}

	m.server()
	return &m
}

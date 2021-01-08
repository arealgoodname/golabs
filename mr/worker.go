package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	ApplyCall(mapf, reducef)

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func ApplyCall(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// declare an argument structure.
	args := new(Assign) //实际上worker不需要给信息

	// declare a reply structure.
	reply := args

	// send the RPC request, wait for the reply.
	if call("Master.Apply", &args, &reply) {
		//fmt.Println(reply.Info)
		if reply.Task {
			if reply.Maptask {
				filename := reply.Info
				intermediate := []KeyValue{}
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
				// test point ,we will produce all the intermediate to one json file
				output(intermediate)
			}
		}
	} else {
		//server shut down all works finished
		fmt.Println("Server down,Worker quit")
	}

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func output(intermediate []KeyValue) bool {
	filename := "test.json"
	fp, err := os.Create(filename)
	if err != nil {
		fmt.Println("Create file failed! ", err)
		return false
	}
	defer fp.Close()
	enc := json.NewEncoder(fp)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println("output failed,err:", err)
			return false
		}
	}
	return true
}

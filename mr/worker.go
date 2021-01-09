package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		ApplyCall(mapf, reducef)
	}
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
	reply := new(Assign)

	// send the RPC request, wait for the reply.
	if call("Master.Apply", &args, &reply) {
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

				tempInter := make([][]KeyValue, 10)
				// test point ,we will produce all the intermediate to one json file
				for _, kv := range intermediate {
					tempInter[ihash(kv.Key)%reply.NReduce] = append(tempInter[ihash(kv.Key)%reply.NReduce], kv)
				}
				for i := 0; i < reply.NReduce; i++ {
					output(tempInter[i], reply.Taskindex, i)
				}
				report := new(Report)
				infoBack := new(Report) //no need  for info back
				report.Maptask = true
				report.Taskindex = reply.Taskindex
				report.Info = reply.Info
				call("Master.Jobdone", report, infoBack)
			} else {
				//Reduce task
				intermediate := []KeyValue{}
				for _, fname := range reply.RedFiles {
					readf(&intermediate, fname)
				}
				//read all the files needed
				sort.Sort(ByKey(intermediate))

				oname := "./output/mr-out-" + strconv.Itoa(reply.Taskindex)
				ofile, _ := os.Create(oname)
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}

				ofile.Close()
			}
		} else {
			//didnt get task,then fucking sleep
			time.Sleep(time.Duration(3) * time.Second)
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

func output(intermediate []KeyValue, mapi int, redi int) bool {
	filename := "./output/mr-" + strconv.Itoa(mapi) + "-" + strconv.Itoa(redi) + ".json"
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

func readf(intermediate *[]KeyValue, fname string) bool {
	filename := "./output/" + fname
	fp, err := os.Open(filename)
	if err != nil {
		fmt.Println("error when open file " + filename)
		return false
	}
	defer fp.Close()

	dec := json.NewDecoder(fp)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		*intermediate = append(*intermediate, kv)
	}
	return true
}

package mr

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/big"
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

type TempFile struct {
	Name string
	Mapi int
	Redi int
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func maybeCrash() {
	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 330 {
		// crash!
		os.Exit(1)
	} else if rr.Int64() < 660 {
		// delay for a while.
		maxms := big.NewInt(10 * 1000)
		ms, _ := crand.Int(crand.Reader, maxms)
		time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
	}
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
	for ApplyCall(mapf, reducef) {
		time.Sleep(3 * time.Second)
	}
	fmt.Println("Server down,worker quit")
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func ApplyCall(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	// declare an argument structure.
	args := new(Assign) //实际上worker不需要给信息

	// declare a reply structure.
	reply := new(Assign)

	// send the RPC request, wait for the reply.
	if call("Master.Apply", &args, &reply) {
		if reply.Task {
			if reply.Maptask {
				fmt.Println("got map task ", reply.Info)
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

				tempfiles := []TempFile{}
				for i := 0; i < reply.NReduce; i++ {
					tf := output(tempInter[i], reply.Taskindex, i)
					tempfiles = append(tempfiles, tf)
				}

				report := new(Report)
				infoBack := new(Confirm) //no need  for info back
				report.Maptask = true
				report.Taskindex = reply.Taskindex
				report.Info = reply.Info
				call("Master.Jobdone", report, infoBack)
				if infoBack.Conf {
					//change of names
					for _, tf := range tempfiles {
						rename(tf)
					}
				}

			} else {
				//Reduce task
				intermediate := []KeyValue{}
				for _, fname := range reply.RedFiles {
					readf(&intermediate, fname)
				}
				//read all the files needed
				sort.Sort(ByKey(intermediate))

				oname := "mr-out-" + strconv.Itoa(reply.Taskindex)
				ofile, _ := ioutil.TempFile("./", oname)
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

				defer ofile.Close()

				report := new(Report)
				infoBack := new(Confirm)
				report.Maptask = false
				report.Taskindex = reply.Taskindex
				report.Info = reply.Info
				call("Master.Jobdone", report, infoBack)
				//handle infoBack
				if infoBack.Conf {
					os.Rename("./"+ofile.Name(), "./mr-out-"+strconv.Itoa(reply.Taskindex))
				}

			}
		} else {
			if reply.Fin {
				return false
			} else {
				time.Sleep(time.Duration(3) * time.Second)
			}
		}
	} else {
		//server shut down all works finished
		return false
	}
	return true
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

func output(intermediate []KeyValue, mapi int, redi int) TempFile {
	tempfile := TempFile{}
	filename := "mr-" + strconv.Itoa(mapi) + "-" + strconv.Itoa(redi) + ".json"
	fp, err := ioutil.TempFile("./", filename)
	if err != nil {
		fmt.Println("Create file failed! ", err)
		return tempfile
	}
	defer fp.Close()
	enc := json.NewEncoder(fp)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println("output failed,err:", err)
			return tempfile
		}
	}
	tempfile.Name = fp.Name()
	tempfile.Mapi = mapi
	tempfile.Redi = redi
	return tempfile
}

func rename(tf TempFile) bool {
	os.Rename("./"+tf.Name, "./mr-"+strconv.Itoa(tf.Mapi)+"-"+strconv.Itoa(tf.Redi)+".json")
	return true
}

func readf(intermediate *[]KeyValue, fname string) bool {
	filename := "./" + fname
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

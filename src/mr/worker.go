package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		t, taskID, nReduce, filename := CallAssignTask()
		if t == 1 {
			DoMap(mapf, taskID, nReduce, filename)
		} else if t == 2 {
			DoReduce(reducef, taskID, filename)
		} else {
			break
		}
	}

}

func DoMap(mapf func(string, string) []KeyValue, taskID int, nReduce int, filename string) {
	fmt.Printf("DoMap taskID = %v, nReduce = %v, filename = %v\n", taskID, nReduce, filename)

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

	kvaBuckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		j := ihash(kv.Key) % nReduce
		kvaBuckets[j] = append(kvaBuckets[j], kv)
	}

	for j := 0; j < nReduce; j++ {
		filename := fmt.Sprintf("mr-%d-%d", taskID, j)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatal("Create:", err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvaBuckets[j] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("Encode:", err)
			}
		}
		file.Close()
	}
	CallNotifyMapDone(taskID)
}

func DoReduce(reducef func(string, []string) string, taskID int, filename string) {
	fmt.Printf("DoReduce taskID = %v, filename = %v\n", taskID, filename)

	files, err := filepath.Glob(filename)
	if err != nil {
		log.Fatal("Glob:", err)
	}
	intermediate := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("Open:", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", taskID)
	ofile, _ := os.Create(oname)

	//fmt.Printf("DoReduce #intermediate = %d\n", len(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

	CallNotifyReduceDone(taskID)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallAssignTask() (int, int, int, string) {

	args := AssignTaskArgs{}

	reply := AssignTaskReply{}

	err := call("Coordinator.AssignTask", &args, &reply)

	if !err {
		return 0, 0, 0, ""
	}

	return reply.TaskType, reply.TaskID, reply.NReduce, reply.Filename
}

func CallNotifyMapDone(taskID int) {

	args := NotifyMapDoneArgs{}

	args.TaskID = taskID

	reply := NotifyMapDoneReply{}

	call("Coordinator.NotifyMapDone", &args, &reply)

}

func CallNotifyReduceDone(taskID int) {

	args := NotifyReduceDoneArgs{}

	args.TaskID = taskID

	reply := NotifyReduceDoneReply{}

	call("Coordinator.NotifyReduceDone", &args, &reply)

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

package mr

import (
	"encoding/json"
	"fmt"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "time"

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

func getRecudeLocalName() string {
	return fmt.Sprintf("mr-temp-%v", os.Getpid())
}

//
// main/mrworker.go calls this function.
//

func mapping(mapf func(string, string) []KeyValue, taskReply *GetTaskReply) []string {
	inputfileName := taskReply.Filenames[0]

	outputFilenames := []string{}
	content, ok := openFile(inputfileName)
	if ok == false {
		return []string{}
	}

	kva := mapf(inputfileName, string(content))

	for _, kv := range kva {
		filename := fmt.Sprintf("mr-%v-%v", taskReply.TaskId, ihash(kv.Key)%taskReply.NReduce)

		_, err := os.Stat(filename)
		if os.IsNotExist(err) {
			outputFilenames = append(outputFilenames, filename)
		}
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println(err)
			log.Fatalf("cannot create %v", filename)
		}

		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			fmt.Printf("Error encoding JSON: %v\n", err)
			continue
		}

		file.Close()
	}
	// fmt.Printf("%v finished\n", inputfileName)
	return outputFilenames
}

func reducing(reducef func(string, []string) string, taskReply *GetTaskReply) string {
	reduceTaskId := taskReply.TaskId
	outputFileName := fmt.Sprintf("mr-out-%v", reduceTaskId)
	intermediate := []KeyValue{}

	for _, filename := range taskReply.Filenames {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err.Error() == "EOF" {
					break
				}
				fmt.Printf("Error decoding JSON: %v\n", err)
				continue
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	tmpFileName := getRecudeLocalName()
	file, _ := os.Create(tmpFileName)

	sort.Sort(ByKey(intermediate))
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

		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	file.Close()

	os.Rename(tmpFileName, outputFileName)

	return outputFileName
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		taskArg := GetTaskArg{}
		taskReply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &taskArg, &taskReply)
		if ok == false {
			break
		}
		// fmt.Println(taskReply.Task, taskReply.TaskId, taskReply.Filenames)
		if taskReply.OK == false {
			time.Sleep(time.Second)
			continue
		}

		var outputFileNames []string
		if taskReply.Task == MAP {
			outputFileNames = mapping(mapf, &taskReply)
		} else if taskReply.Task == REDUCE {
			outputFileNames = append(outputFileNames, reducing(reducef, &taskReply))
		} else {
			break
		}
		// fmt.Println(taskReply.Task, taskReply.TaskId, outputFileNames)
		finishTaskArg := FinishTaskArg{}
		finishTaskReply := FinishTaskReply{}
		finishTaskArg.Task = taskReply.Task
		finishTaskArg.TaskId = taskReply.TaskId
		finishTaskArg.RetFilenames = outputFileNames

		call("Coordinator.FinishTask", &finishTaskArg, &finishTaskReply)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func openFile(filename string) ([]byte, bool) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return make([]byte, 0), false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return make([]byte, 0), false
	}
	file.Close()
	return content, true
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

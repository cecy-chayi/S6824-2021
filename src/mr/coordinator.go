package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"
import "strings"
import "strconv"
import "sync"

type Coordinator struct {
	// Your definitions here.
	MapTaskQueue       []string
	MapTaskOccupied    []bool
	MapTaskFinished    []bool
	MapTaskFinishedNum int
	MapMutex           sync.Mutex

	ReduceTaskQueue       [][]string
	ReduceTaskOccupied    []bool
	ReduceTaskFinished    []bool
	ReduceTaskFinishedNum int
	ReduceMutex           sync.Mutex

	NReduce int
}

const (
	TIME_OUT = 10 * time.Second
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) taskStateCheck(tt TaskType, id int) {
	var taskOccupied []bool
	var taskFinished []bool
	var mutex *sync.Mutex
	if tt == MAP {
		mutex = &c.MapMutex
		taskOccupied = c.MapTaskOccupied
		taskFinished = c.MapTaskFinished
	} else {
		mutex = &c.ReduceMutex
		taskOccupied = c.ReduceTaskOccupied
		taskFinished = c.ReduceTaskFinished
	}
	mutex.Lock()
	defer mutex.Unlock()
	if taskFinished[id] == false {
		taskOccupied[id] = false
		fmt.Printf("task timeout\n")
	}
}

func (c *Coordinator) findTask(tt TaskType) (int, bool) {
	if tt == MAP {
		c.MapMutex.Lock()
		defer c.MapMutex.Unlock()
		for i, val := range c.MapTaskOccupied {
			if val == false && c.MapTaskFinished[i] == false {
				c.MapTaskOccupied[i] = true
				return i, true
			}
		}
	} else {
		c.ReduceMutex.Lock()
		defer c.ReduceMutex.Unlock()

		for i, val := range c.ReduceTaskOccupied {
			if val == false && c.ReduceTaskFinished[i] == false {
				c.ReduceTaskOccupied[i] = true
				return i, true
			}
		}
	}
	return -1, false
}

func (c *Coordinator) GetTask(args *GetTaskArg, reply *GetTaskReply) error {
	reply.NReduce = c.NReduce
	c.MapMutex.Lock()
	MapTaskFinishedNum := c.MapTaskFinishedNum
	c.MapMutex.Unlock()
	if MapTaskFinishedNum != len(c.MapTaskQueue) {
		id, ok := c.findTask(MAP)
		if ok == false {
			reply.OK = false
			// fmt.Printf("no ready map task\n")
			return nil
		}
		reply.Task = MAP
		reply.TaskId = id
		reply.Filenames = append(reply.Filenames, c.MapTaskQueue[id])
		reply.OK = true
		time.AfterFunc(TIME_OUT, func() {
			c.taskStateCheck(MAP, id)
		})
		return nil
	}
	c.ReduceMutex.Lock()
	ReduceTaskFinishedNum := c.ReduceTaskFinishedNum
	c.ReduceMutex.Unlock()
	if ReduceTaskFinishedNum != c.NReduce {
		id, ok := c.findTask(REDUCE)
		if ok == false {
			reply.OK = false
			// fmt.Printf("no ready reduce task\n")
			return nil
		}
		reply.Task = REDUCE
		reply.TaskId = id
		reply.Filenames = c.ReduceTaskQueue[id]
		reply.OK = true
		time.AfterFunc(TIME_OUT, func() {
			c.taskStateCheck(REDUCE, id)
		})
		return nil
	}
	reply.Task = QUIT
	reply.OK = true
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArg, reply *FinishTaskReply) error {
	reply.OK = true
	
	if args.Task == MAP {
		c.MapMutex.Lock()
		if c.MapTaskFinished[args.TaskId] == false {
			
			c.MapTaskFinished[args.TaskId] = true
			c.MapTaskFinishedNum++
			c.MapMutex.Unlock()
			// fmt.Println(args.RetFilenames)
			for _, retFilename := range args.RetFilenames {
				parts := strings.Split(retFilename, "-")
				reduceTaskId, _ := strconv.Atoi(parts[2])

				c.ReduceMutex.Lock()
				c.ReduceTaskQueue[reduceTaskId] = append(c.ReduceTaskQueue[reduceTaskId], retFilename)
				c.ReduceMutex.Unlock()
			}
		} else {
			c.MapMutex.Unlock()
		}
	} else {
		parts := strings.Split(args.RetFilenames[0], "-")
		reduceTaskId, _ := strconv.Atoi(parts[2])
		c.ReduceMutex.Lock()
		c.ReduceTaskFinished[reduceTaskId] = true
		c.ReduceTaskFinishedNum++
		c.ReduceMutex.Unlock()
	}


	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()
	ret = c.ReduceTaskFinishedNum == c.NReduce
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapTaskQueue = files
	c.MapTaskOccupied = make([]bool, len(files))
	c.MapTaskFinished = make([]bool, len(files))

	c.ReduceTaskQueue = make([][]string, nReduce)
	c.ReduceTaskOccupied = make([]bool, nReduce)
	c.ReduceTaskFinished = make([]bool, nReduce)

	c.NReduce = nReduce
	c.ReduceTaskFinishedNum = 0
	c.server()
	return &c
}

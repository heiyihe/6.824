package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	Map = iota
	Reduce
)

var numberReduce int
var inputFileSize int
var jobId = 0

type Coordinator struct {
	finish           int
	completedFile    map[int]bool
	completedReduce  map[int]bool
	outputFile       []string
	intermediateFile []string
	mapJob           chan *Reply
	reduceJob        chan *Reply
	// Your definitions here.
}

type Reply struct {
	JobType   int
	Filename  []string
	MapNum    int
	ReduceNum int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Distribute(request *Request, reply *Reply) error {
	if c.finish < inputFileSize {
		reply.JobType = Map

	} else if c.finish == inputFileSize {
		reply.JobType = Reduce

	}
}

func (c *Coordinator) MakeMapJob(fileName []string) error {
	if len(fileName) <= 0 {
		return "There is no input file"
	}
	for _, file := range fileName {
		id := c.genId(0)
		reply := Reply{
			JobType:   Map,
			Filename:  []string{file},
			MapNum:    id,
			ReduceNum: -1,
		}
		c.mapJob <- &reply
	}
	fmt.Println("MapJobs have already created")
	return nil
}

func (c *Coordinator) genId(flag int) (id int) {
	if flag == 1 {
		jobId = 0
	}
	jobId++
	return jobId
}

//save the number of Map workers, send Reduce Number back
func (c *Coordinator) Init(nmap int, nReduce *int) error {
	numberMap = nmap
	if numberReduce == 0 {
		return "no nReduce info"
	}
	nReduce = new(numberReduce)
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

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	inputFileSize = len(files)
	// Your code here.
	numberReduce = nReduce

	c.server()
	return &c
}

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

const (
	MapJob = iota
	ReduceJob
	WaitJob
	EndJob
)

var mapMu sync.Mutex
var reduceMu sync.Mutex
var jobId = 0

type Coordinator struct {
	completedMap     map[int]bool
	completedReduce  map[int]bool
	outputFile       []string
	intermediateFile map[int][]string
	mapJobQueue      chan *JobInfo
	reduceJobQueue   chan *JobInfo
	reduceTaskNum    int
	mapTaskNum       int
	// Your definitions here.
}

type JobInfo struct {
	JobType    int
	InputFile  []string
	OutputFile []string
	MapNum     int
	ReduceNum  int
}

//Print JobInfo for test
func (job *JobInfo) Printf() {
	fmt.Println(job.ReduceNum)
	fmt.Println(job.InputFile)
	fmt.Println(job.MapNum)
}

func (c *Coordinator) GetMapSize() (size int) {
	mapMu.Lock()
	length := len(c.completedMap)
	defer mapMu.Unlock()
	return length
}

func (c *Coordinator) SearchMap(mapNum int) (flag bool) {
	mapMu.Lock()
	_, ok := c.completedMap[mapNum]
	defer mapMu.Unlock()
	return ok
}

func (c *Coordinator) SetMap(mapNum int) {
	mapMu.Lock()
	c.completedMap[mapNum] = true
	defer mapMu.Unlock()
}

func (c *Coordinator) GetReduceSize() (size int) {
	reduceMu.Lock()
	length := len(c.completedReduce)
	defer reduceMu.Unlock()
	return length
}

func (c *Coordinator) SearchReduce(reduceNum int) (flag bool) {
	reduceMu.Lock()
	_, ok := c.completedReduce[reduceNum]
	defer reduceMu.Unlock()
	return ok
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Distribute(request *Args, reply *JobInfo) error {
	if c.GetMapSize() < c.mapTaskNum {
		if len(c.mapJobQueue) > 0 {
			*reply = *<-c.mapJobQueue
			//开启新的线程监视是否在规定时间内完成了分配的任务
			go func() {
				repeatJob := *reply
				time.Sleep(10 * time.Second)
				if !c.SearchMap(repeatJob.MapNum) {
					c.mapJobQueue <- &repeatJob
				}
				return
			}()
		} else {
			reply.JobType = WaitJob
		}
	} else if c.GetMapSize() == c.mapTaskNum && c.GetReduceSize() < c.reduceTaskNum {
		if len(c.reduceJobQueue) > 0 {
			*reply = *<-c.reduceJobQueue
			reply.InputFile = c.intermediateFile[reply.ReduceNum]
			go func() {
				repeatJob := *reply
				time.Sleep(5 * time.Second)
				if !c.SearchReduce(repeatJob.ReduceNum) {
					c.reduceJobQueue <- &repeatJob
				}
				return
			}()
		} else {
			reply.JobType = WaitJob
		}
	} else {
		reply.JobType = EndJob
	}
	return nil
}

//worker完成一项任务后调用该函数
func (c *Coordinator) JobDone(jobInfo *JobInfo, reply *Args) error {
	switch jobInfo.JobType {
	case MapJob:
		c.SetMap(jobInfo.MapNum)
		fmt.Printf("MapJob %d has finished\n", jobInfo.MapNum)
		for i := 0; i < c.reduceTaskNum; i++ {
			c.intermediateFile[i] = append(c.intermediateFile[i], jobInfo.OutputFile[i])
		}
	case ReduceJob:
		reduceMu.Lock()
		fmt.Printf("ReduceJob %d has finished\n", jobInfo.ReduceNum)
		c.completedReduce[jobInfo.ReduceNum] = true
		defer reduceMu.Unlock()
	default:
		panic("There is no such JobType")
	}
	return nil
}

//制作MapJob
func (c *Coordinator) MakeMapJob(fileName []string, nReduce int) error {
	if len(fileName) <= 0 {
		return fmt.Errorf("There is no input file")
	}
	for _, file := range fileName {
		id := c.genId(0)
		mapJob := JobInfo{
			JobType:    MapJob,
			InputFile:  []string{file},
			OutputFile: []string{},
			MapNum:     id,
			ReduceNum:  nReduce,
		}
		c.mapJobQueue <- &mapJob
	}
	fmt.Println("MapJobs have already created\n")
	return nil
}

//制作ReduceJob
func (c *Coordinator) MakeReduceJob(nReduce int) error {
	for i := 0; i < nReduce; i++ {
		reduceJob := JobInfo{
			JobType:    ReduceJob,
			InputFile:  []string{},
			OutputFile: []string{"mr-out-" + strconv.Itoa(i)},
			MapNum:     -1,
			ReduceNum:  i,
		}
		c.reduceJobQueue <- &reduceJob
	}
	fmt.Println("ReduceJobs have already created\n")
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
	if c.GetReduceSize() == c.reduceTaskNum {
		ret = true
		time.Sleep(5 * time.Second)
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	inputFileSize := len(files)
	if inputFileSize == 0 {
		panic("There is no input file")
	}
	//Coordinator初始化
	c := Coordinator{
		completedMap:     make(map[int]bool),
		completedReduce:  make(map[int]bool),
		outputFile:       []string{},
		intermediateFile: make(map[int][]string),
		mapJobQueue:      make(chan *JobInfo, inputFileSize),
		reduceJobQueue:   make(chan *JobInfo, nReduce),
		mapTaskNum:       inputFileSize,
		reduceTaskNum:    nReduce,
	}
	c.MakeMapJob(files, nReduce)
	c.MakeReduceJob(nReduce)
	// Your code here.
	c.server()
	return &c
}

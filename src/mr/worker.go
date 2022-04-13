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

var filepath = "//home/chujh/mit/6.824/src/main/map-tmp"

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

	for {
		request := Args{}
		var reply JobInfo
		// worker send a call for task to coordinator
		//根据返回的Job类型运行相应程序
		ok := call("Coordinator.Distribute", &request, &reply)
		if ok {
			switch reply.JobType {
			case MapJob:
				MapService(reply, mapf)
			case ReduceJob:
				ReduceService(reply, reducef)
			case WaitJob:
				time.Sleep(time.Second)
			case EndJob:
				fmt.Println("MapReduce Job has finished")
				return
			default:
				panic("Invalid jobType")
			}
		} else {
			return
		}
	}

	// Your worker implementation here.
	// divide the input
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func MapService(mapJob JobInfo, mapf func(string, string) []KeyValue) {

	//Transfer the input file into KV-Pairs
	file, err := os.Open(mapJob.InputFile[0])
	if err != nil {
		log.Fatalf("cannot open %v", mapJob.InputFile[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapJob.InputFile[0])
	}
	file.Close()
	kva := mapf(mapJob.InputFile[0], string(content))

	nReduce := mapJob.ReduceNum
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		//filename := "mr-tmp-" + strconv.Itoa(i)
		//outFiles[i], err = os.Create(filename)
		outFiles[i], err = ioutil.TempFile(filepath, "map-tmp-*")
		fileEncs[i] = json.NewEncoder(outFiles[i])
	}

	interFile := "mr-" + strconv.Itoa(mapJob.MapNum) + "-"
	for _, pair := range kva {
		reduceNum := ihash(pair.Key) % nReduce
		err := fileEncs[reduceNum].Encode(&pair)
		//若kv写入失败，删除所有临时文件
		if err != nil {
			fmt.Printf("Key %v Value %v writes fail, err message: %v", pair.Key, pair.Value, err)
			os.RemoveAll("map-tmp")
			panic("Json encode failed")
		}
	}
	for outindex, file := range outFiles {
		outname := interFile + strconv.Itoa(outindex)
		//fmt.Printf("temp file oldpath %v\n", oldpath)
		os.Remove(outname)
		os.Rename(file.Name(), outname)
		mapJob.OutputFile = append(mapJob.OutputFile, outname)
		file.Close()
	}

	RecallDone(&mapJob)
}

func ReduceService(reduceJob JobInfo, reducef func(string, []string) string) {
	kva := []KeyValue{}
	fmt.Println(reduceJob.InputFile)
	for _, fileName := range reduceJob.InputFile {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	ofile, err := ioutil.TempFile(filepath, "mr-*")
	if err != nil {
		fmt.Printf("Reduce task %d create output file failed: %v\n", reduceJob.ReduceNum, err)
		os.RemoveAll(filepath)
		return
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	outname := "mr-out-" + strconv.Itoa(reduceJob.ReduceNum)
	os.Remove(outname)
	os.Rename(ofile.Name(), outname)
	ofile.Close()
	reduceJob.OutputFile = []string{ofile.Name()}
	RecallDone(&reduceJob)
}

//告诉Coordinator任务已经完成
func RecallDone(jobInfo *JobInfo) {
	request := Args{}
	call("Coordinator.JobDone", jobInfo, &request)
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

package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
)

// implement sort
type keyValues []KeyValue

func (pair keyValues) Len() int {
	return len(pair)
}
func (pair keyValues) Swap(i, j int) {
	pair[i], pair[j] = pair[j], pair[i]
}
func (pair keyValues) Less(i, j int) bool {
	return pair[i].Key < pair[j].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// read the intermediate files for the task
	var keyValues keyValues
	for m := 0; m < nMap; m++ {
		inFileName := reduceName(jobName, m, reduceTask)

		inFile, _ := os.OpenFile(inFileName, os.O_RDONLY, 0755)
		defer inFile.Close()

		dec := json.NewDecoder(inFile)
		for {
			var keyValue KeyValue
			if err := dec.Decode(&keyValue); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			keyValues = append(keyValues, keyValue)
		}
	}

	// sort the intermediate key/value pairs by key
	sort.Sort(keyValues)
	pairMap := make(map[string][]string)
	for _, value := range keyValues {
		pair := append(pairMap[value.Key], value.Value)
		pairMap[value.Key] = pair
	}

	// create encoder
	file, _ := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	enc := json.NewEncoder(file)
	defer file.Close()

	for key, value := range pairMap {
		// call the user-defined reduce function (reduceF) for each key
		val := reduceF(key, value)

		// and write reduceF's output to disk
		_ = enc.Encode(&KeyValue{key, val})
	}
}

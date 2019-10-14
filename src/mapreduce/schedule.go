package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var wg sync.WaitGroup
	workerCh := make(chan chan int)
	failCh := make(chan int)

	// Handling worker failures
	go func() {
		for index := range failCh {
			wg.Add(1)
			ch := <-workerCh
			ch <- index
		}
	}()

	go func() {
		for worker := range registerChan {
			ch := make(chan int)
			workerCh <- ch

			go func(worker string, ch chan int) {
				for index := range ch {
					res := call(worker, "Worker.DoTask",
						DoTaskArgs{jobName, mapFiles[index], phase, index, n_other}, nil)

					// Handling worker failures
					if res == false {
						failCh <- index
					}
					wg.Done()
					workerCh <- ch
				}
			}(worker, ch)
		}
	}()

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		ch := <-workerCh
		ch <- i
	}
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}

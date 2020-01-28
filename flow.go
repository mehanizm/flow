package flow

import (
	"sync"
)

// Flow is the abstract flow worker
// to operate input, output and process
type Flow struct {
	In      map[string]Reader
	Out     map[string]Writer
	Process map[string]Processor
}

const chanBuffer = 100

// NewFlow initialize new flow instance
func NewFlow() *Flow {
	return &Flow{
		In:      make(map[string]Reader, 0),
		Out:     make(map[string]Writer, 0),
		Process: make(map[string]Processor, 0),
	}
}

// Reader input data to flow
type Reader interface {
	ReadDataToChan() (inChan chan *map[string]string)
}

// Writer output data from flow
type Writer interface {
	WriteDataFromChan(wg *sync.WaitGroup, outChan chan *map[string]string)
}

// Processor data in flow
type Processor interface {
	ProcessMessage(wg *sync.WaitGroup, inChan, outChan chan *map[string]string, goroutineNum int)
}

// SetInFlow in setter
func (f *Flow) SetInFlow(in map[string]Reader) {
	f.In = in
}

// AddInFlow in adder
func (f *Flow) AddInFlow(key string, in Reader) {
	f.In[key] = in
}

// SetOutFlow out setter
func (f *Flow) SetOutFlow(out map[string]Writer) {
	f.Out = out
}

// AddOutFlow out adder
func (f *Flow) AddOutFlow(key string, out Writer) {
	f.Out[key] = out
}

// SetProcessFlow process setter
func (f *Flow) SetProcessFlow(process map[string]Processor) {
	f.Process = process
}

// AddProcessFlow process adder
func (f *Flow) AddProcessFlow(key string, process Processor) {
	f.Process[key] = process
}

// Serve flow in concurrent mode
func (f *Flow) Serve(workersCount int, in, out, process string) {

	inChan := f.In[in].ReadDataToChan()
	outChan := make(chan *map[string]string, chanBuffer)

	wgWorkers := &sync.WaitGroup{}
	wgWriter := &sync.WaitGroup{}

	for workerNum := 0; workerNum < workersCount; workerNum++ {
		wgWorkers.Add(1)
		go f.Process[process].ProcessMessage(wgWorkers, inChan, outChan, workerNum)
	}

	wgWriter.Add(1)
	go f.Out[out].WriteDataFromChan(wgWriter, outChan)

	wgWorkers.Wait()
	close(outChan)

	wgWriter.Wait()

}

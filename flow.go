package flow

import (
	"fmt"
	"sync"
)

const chanBufferDefault = 100

// Flow is the abstract flow worker
// to operate input, output and process
type Flow struct {
	mu         sync.Mutex
	chanBuffer uint16
	In         map[string]Reader
	Out        map[string]Writer
	Process    map[string]Processor
}

// NewFlow initialize new flow instance
func NewFlow() *Flow {
	return &Flow{
		mu:         sync.Mutex{},
		chanBuffer: chanBufferDefault,
		In:         make(map[string]Reader, 0),
		Out:        make(map[string]Writer, 0),
		Process:    make(map[string]Processor, 0),
	}
}

func (f *Flow) WithChanBuffer(chanBuffer uint16) *Flow {
	f.chanBuffer = chanBuffer
	return f
}

// Reader input data to flow
type Reader interface {
	ReadDataToChan() (inChan chan map[string]string)
	Cancel()
}

// Writer output data from flow
type Writer interface {
	WriteDataFromChan(wg *sync.WaitGroup, outChan chan map[string]string)
}

// Processor data in flow
type Processor interface {
	ProcessMessage(wg *sync.WaitGroup, inChan, outChan chan map[string]string, goroutineNum int)
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

func (f *Flow) Stop(in string) error {
	reader, ok := f.In[in]
	if !ok {
		return fmt.Errorf("There is no InFlow with the specified key: %v", in)
	}
	reader.Cancel()
	return nil
}

// Serve flow in concurrent mode
func (f *Flow) Serve(workersCount int, in, out string, processors []string) error {

	reader, ok := f.In[in]
	if !ok {
		return fmt.Errorf("There is no InFlow with the specified key: %v", in)
	}

	writer, ok := f.Out[out]
	if !ok {
		return fmt.Errorf("There is no OutFlow with the specified key: %v", out)
	}

	processorsCount := len(processors)
	wgWorkers := make([]*sync.WaitGroup, 0)
	processorsChannels := []chan map[string]string{reader.ReadDataToChan()}

	for processorNum, processorName := range processors {

		processor, ok := f.Process[processorName]
		if !ok {
			return fmt.Errorf("There is no Processor with the specified key: %v", processorName)
		}

		wgWorkers = append(wgWorkers, &sync.WaitGroup{})
		processorsChannels = append(processorsChannels, make(chan map[string]string, f.chanBuffer))

		for workerNum := 0; workerNum < workersCount; workerNum++ {
			wgWorkers[processorNum].Add(1)
			go processor.ProcessMessage(wgWorkers[processorNum], processorsChannels[processorNum], processorsChannels[processorNum+1], workerNum)
		}
	}

	wgWriter := &sync.WaitGroup{}
	wgWriter.Add(1)
	go writer.WriteDataFromChan(wgWriter, processorsChannels[processorsCount])

	for wgWorkerNum, wgWorker := range wgWorkers {
		wgWorker.Wait()
		close(processorsChannels[wgWorkerNum+1])
	}

	wgWriter.Wait()

	return nil

}

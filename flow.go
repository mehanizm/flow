package flow

import (
	"fmt"
	"sync"
	"time"
)

const (
	chanBufferDefault = 100
	waitToKillDefault = 60
)

// nolint:structcheck
type flowConfig struct {
	in, out      string
	processors   []string
	chanBuffer   uint16
	waitToKill   uint16
	workersCount int
}

// Flow is the abstract flow worker
// to operate input, output and process
type Flow struct {
	mu sync.Mutex
	flowConfig
	status  *flowStatus
	In      map[string]Reader
	Out     map[string]Writer
	Process map[string]Processor
}

// NewFlow initialize new flow instance
func NewFlow() *Flow {
	return &Flow{
		flowConfig: flowConfig{
			chanBuffer: chanBufferDefault,
			waitToKill: waitToKillDefault,
		},
		status:  newFlowStatus(),
		mu:      sync.Mutex{},
		In:      make(map[string]Reader),
		Out:     make(map[string]Writer),
		Process: make(map[string]Processor),
	}
}

func (f *Flow) WithChanBuffer(chanBuffer uint16) *Flow {
	f.chanBuffer = chanBuffer
	return f
}

func (f *Flow) WithWaitToKill(waitToKill uint16) *Flow {
	f.waitToKill = waitToKill
	return f
}

func (f *Flow) IsRunning() bool {
	return f.status.isRunning()
}

// Reader input data to flow
type Reader interface {
	ReadDataToChan() (inChan chan map[string]string)
	Cancel()
	GetReadStatus() (countRead, countMax uint64)
}

// Writer output data from flow
type Writer interface {
	WriteDataFromChan(wg *sync.WaitGroup, outChan chan map[string]string)
	IsFinished() <-chan struct{}
	GetWriteStatus() (countWrite uint64)
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

// Stop stops reading
// important that Reader should be tollerant to Cancel
// if it is not reading
func (f *Flow) Stop() error {
	err := f.status.cancelling()
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.In[f.in].Cancel()
	select {
	case <-f.Out[f.out].IsFinished():
	case <-time.After(time.Duration(f.waitToKill) * time.Second):
	}
	f.status.cancell()
	f.mu.Unlock()
	return nil
}

func (f *Flow) getReadCounts() (countRead, countWrite, countMax uint64) {
	countRead, countMax = f.In[f.in].GetReadStatus()
	countWrite = f.Out[f.out].GetWriteStatus()
	return
}

func (f *Flow) GetStatus() FlowStatus {
	f.status.updateCounts(f.getReadCounts())
	return f.status.get()
}

// Serve flow in concurrent mode
func (f *Flow) Serve(workersCount int, in, out string, processors []string) error {
	err := f.status.start()
	if err != nil {
		return err
	}
	err = f.serve(workersCount, in, out, processors)
	switch {
	case err != nil:
		f.status.error(err.Error())
	case f.IsRunning():
		f.status.finish()
	default:
	}

	return err
}

func (f *Flow) Restart() error {
	return f.status.restart()
}

func (f *Flow) serve(workersCount int, in, out string, processors []string) error {
	f.mu.Lock()
	f.in = in
	f.out = out
	f.processors = processors
	f.workersCount = workersCount
	reader, ok := f.In[in]
	if !ok {
		return fmt.Errorf("There is no InFlow with the specified key: %v", in)
	}
	writer, ok := f.Out[out]
	if !ok {
		return fmt.Errorf("There is no OutFlow with the specified key: %v", out)
	}
	f.mu.Unlock()

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

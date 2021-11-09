package flow

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const testSize = 100

type mockReader struct {
	mu        sync.Mutex
	cancel    chan struct{}
	isReading bool
	sleep     int
}

func newMockReader(sleep int) *mockReader {
	return &mockReader{
		mu:     sync.Mutex{},
		cancel: make(chan struct{}, 2),
		sleep:  sleep,
	}
}

func (mr *mockReader) ReadDataToChan() (inChan chan map[string]string) {
	out := make(chan map[string]string, 1)
	go func() {
		mr.mu.Lock()
		mr.isReading = true
		mr.mu.Unlock()
	LOOP:
		for i := 0; i < testSize; i++ {
			select {
			case <-mr.cancel:
				break LOOP
			default:
			}
			time.Sleep(time.Duration(mr.sleep) * time.Millisecond)
			out <- map[string]string{"number1": fmt.Sprintf("%v", i)}
		}
		mr.mu.Lock()
		defer mr.mu.Unlock()
		mr.isReading = false
		close(out)
	}()
	return out
}

func (mr *mockReader) Cancel() {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if mr.isReading {
		mr.cancel <- struct{}{}
	}
}

func (mr *mockReader) GetReadStatus() (countRead, countMax uint64) {
	return 0, 0
}

type mockWriter struct {
	isFinished chan struct{}
}

func (mw *mockWriter) IsFinished() <-chan struct{} {
	return mw.isFinished
}

func (mw *mockWriter) WriteDataFromChan(wg *sync.WaitGroup, outChan chan map[string]string) {
	if mw.isFinished == nil {
		mw.isFinished = make(chan struct{}, 1)
	}
	defer wg.Done()
	count := 0
	for m := range outChan {
		count++
		if _, ok := m["number1"]; !ok {
			panic(fmt.Sprintf("mistake in test, number1: %+v", m))
		}
		if _, ok := m["number2"]; !ok {
			panic(fmt.Sprintf("mistake in test, number2: %+v", m))
		}
		if _, ok := m["number3"]; !ok {
			panic(fmt.Sprintf("mistake in test, number3: %+v", m))
		}
		if m["number1"] != m["number2"] || m["number2"] != m["number3"] {
			panic(fmt.Sprintf("mistake in test, does not equal: %+v", m))
		}
	}
	if count == 4 {
		fmt.Println("it was cancel event")
	} else if count != testSize {
		panic(fmt.Sprintf("mistake in test, wrong test_size: %v", count))
	}
	mw.isFinished <- struct{}{}
	close(mw.isFinished)
}

type mockProcess1 struct{}

func (mp1 *mockProcess1) ProcessMessage(wg *sync.WaitGroup, inChan, outChan chan map[string]string, goroutineNum int) {
	defer wg.Done()
	for m := range inChan {
		m["number2"] = m["number1"]
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(100) //nolint:gosec
		time.Sleep(time.Duration(n) * time.Millisecond)
		outChan <- m
	}
}

type mockProcess2 struct{}

func (mp2 *mockProcess2) ProcessMessage(wg *sync.WaitGroup, inChan, outChan chan map[string]string, goroutineNum int) {
	defer wg.Done()
	for m := range inChan {
		m["number3"] = m["number1"]
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(100) //nolint:gosec
		time.Sleep(time.Duration(n) * time.Millisecond)
		outChan <- m
	}
}

func TestFlow_Serve(t *testing.T) {
	flow := NewFlow()
	flow.AddInFlow("in", newMockReader(0))
	flow.AddOutFlow("out", &mockWriter{})
	flow.AddProcessFlow("1", &mockProcess1{})
	flow.AddProcessFlow("2", &mockProcess2{})

	err := flow.Serve(5, "in", "out", []string{"1", "2"})
	if err != nil {
		t.Error("was error", err)
	}

	status, _, _, _ := flow.GetStatus()
	if status != FINISHED {
		t.Errorf("wrong status: %s", status)
	}
}

func TestFlow_ServeWithCancel(t *testing.T) {
	flow := NewFlow().WithChanBuffer(1)
	flow.AddInFlow("in", newMockReader(100))
	flow.AddOutFlow("out", &mockWriter{})
	flow.AddProcessFlow("1", &mockProcess1{})
	flow.AddProcessFlow("2", &mockProcess2{})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(400 * time.Millisecond)
		err := flow.Stop()
		if err != nil {
			t.Error("was error", err)
		}
		err = flow.Stop()
		if err == nil {
			t.Error("should be an error but not")
		}
	}()
	err := flow.Serve(2, "in", "out", []string{"1", "2"})
	if err != nil {
		t.Error("was error", err)
	}
	wg.Wait()
	status, _, _, _ := flow.GetStatus()
	if status != CANCELLED {
		t.Fatal("status was not cancelled", status)
	}
}

func TestFlow_ServerWithError(t *testing.T) {
	flow := NewFlow()
	flow.SetInFlow(map[string]Reader{"in": newMockReader(0)})
	flow.SetOutFlow(map[string]Writer{"out": &mockWriter{}})
	flow.SetProcessFlow(map[string]Processor{"1": &mockProcess1{}, "2": &mockProcess2{}})

	err := flow.Serve(1, "in_not_exist", "out", []string{"1", "2"})
	if err == nil {
		t.Error("should be an error in InFlow but nil")
	}

	err = flow.Serve(1, "in", "out_not_exist", []string{"1", "2"})
	if err == nil {
		t.Error("should be an error in OutFlow but nil")
	}

	err = flow.Serve(1, "in", "out", []string{"1", "2_not_exist"})
	if err == nil {
		t.Error("should be an error in ProcessFlow but nil")
	}
}

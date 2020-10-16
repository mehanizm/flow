package flow

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const testSize = 100

type mockReader struct{}

func (mr *mockReader) ReadDataToChan() (inChan chan map[string]string) {
	out := make(chan map[string]string)
	go func() {
		for i := 0; i < testSize; i++ {
			out <- map[string]string{"number1": fmt.Sprintf("%v", i)}
		}
		close(out)
	}()
	return out
}

type mockWriter struct{}

func (mw *mockWriter) WriteDataFromChan(wg *sync.WaitGroup, outChan chan map[string]string) {
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
		// fmt.Println(m)
	}
	if count != testSize {
		panic(fmt.Sprintf("mistake in test, wrong test_size: %v", count))
	}
}

type mockProcess1 struct{}

func (mp1 *mockProcess1) ProcessMessage(wg *sync.WaitGroup, inChan, outChan chan map[string]string, goroutineNum int) {
	defer wg.Done()
	for m := range inChan {
		m["number2"] = m["number1"]
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(100)
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
		n := rand.Intn(100)
		time.Sleep(time.Duration(n) * time.Millisecond)
		outChan <- m
	}
}

func TestFlow_Serve(t *testing.T) {

	flow := NewFlow()
	flow.AddInFlow("in", &mockReader{})
	flow.AddOutFlow("out", &mockWriter{})
	flow.AddProcessFlow("1", &mockProcess1{})
	flow.AddProcessFlow("2", &mockProcess2{})

	err := flow.Serve(5, "in", "out", []string{"1", "2"})
	if err != nil {
		t.Error("was error", err)
	}

}

func TestFlow_ServerError(t *testing.T) {

	flow := NewFlow()
	flow.SetInFlow(map[string]Reader{"in": &mockReader{}})
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
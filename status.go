package flow

import (
	"fmt"
	"sync"
	"time"
)

type Status uint8

var statusMap = map[uint8]string{
	0: "NOT_EXIST",
	1: "WAIT_TO_START",
	2: "PROCESSING",
	3: "ERROR",
	4: "CANCELLING",
	5: "CANCELLED",
	6: "FINISHED",
}

func (s Status) String() string {
	return statusMap[uint8(s)]
}

const (
	NOT_EXIST Status = iota
	WAIT_TO_START
	PROCESSING
	ERROR
	CANCELLING
	CANCELLED
	FINISHED
)

type flowStatus struct {
	mu             sync.Mutex
	status         Status
	started, ended time.Time
	description    string
}

func newFlowStatus() *flowStatus {
	return &flowStatus{
		mu:     sync.Mutex{},
		status: WAIT_TO_START,
	}
}

func (fs *flowStatus) isStartable() bool {
	return fs.status == WAIT_TO_START || fs.status == FINISHED
}

func (fs *flowStatus) isCancellable() bool {
	return fs.status == PROCESSING
}

func (fs *flowStatus) isRunning() bool {
	return fs.status == PROCESSING
}

func (fs *flowStatus) start() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if !fs.isStartable() {
		return fmt.Errorf("cannot start because the Flow is in status: %s", fs.status)
	}
	fs.status = PROCESSING
	fs.started = time.Now()
	fs.ended = time.Time{}
	fs.description = ""
	return nil
}

func (fs *flowStatus) cancelling() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if !fs.isCancellable() {
		return fmt.Errorf("cannot cancell because the Flow is in status: %s", fs.status)
	}
	fs.status = CANCELLING
	return nil
}

func (fs *flowStatus) cancell() {
	fs.mu.Lock()
	fs.status = CANCELLED
	fs.ended = time.Now()
	fs.mu.Unlock()
}

func (fs *flowStatus) error(err string) {
	fs.mu.Lock()
	fs.status = ERROR
	fs.ended = time.Now()
	fs.description = err
	fs.mu.Unlock()
}

func (fs *flowStatus) finish() {
	fs.mu.Lock()
	fs.status = FINISHED
	fs.ended = time.Now()
	fs.mu.Unlock()
}

func (fs *flowStatus) restart() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if !fs.isStartable() {
		return fmt.Errorf("cannot restart flow, because status is: %v", fs.status)
	}
	fs = &flowStatus{
		mu:     sync.Mutex{},
		status: WAIT_TO_START,
	}
	return nil
}

func (fs *flowStatus) get() (Status, time.Time, time.Time, string) {
	fs.mu.Lock()
	status, started, ended, desc :=
		fs.status, fs.started, fs.ended, fs.description
	fs.mu.Unlock()
	return status, started, ended, desc
}

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
	countRead      uint64
	countWrite     uint64
	countMax       uint64
}

type FlowStatus struct {
	Status         Status
	Started, Ended time.Time
	Description    string
	CountRead      uint64
	CountWrite     uint64
	CountMax       uint64
}

func newFlowStatus() *flowStatus {
	return &flowStatus{
		mu:     sync.Mutex{},
		status: WAIT_TO_START,
	}
}

func (fs *flowStatus) updateCounts(countRead, countWrite, countMax uint64) {
	fs.mu.Lock()
	fs.countRead = countRead
	fs.countWrite = countWrite
	fs.countMax = countMax
	fs.mu.Unlock()
}

func (fs *flowStatus) isStartable() bool {
	fs.mu.Lock()
	status := fs.status
	fs.mu.Unlock()
	return status == WAIT_TO_START
}

func (fs *flowStatus) isRestartable() bool {
	fs.mu.Lock()
	status := fs.status
	fs.mu.Unlock()
	return status == WAIT_TO_START || status == FINISHED
}

func (fs *flowStatus) isCancellable() bool {
	fs.mu.Lock()
	status := fs.status
	fs.mu.Unlock()
	return status == WAIT_TO_START || status == PROCESSING
}

func (fs *flowStatus) isRunning() bool {
	fs.mu.Lock()
	status := fs.status
	fs.mu.Unlock()
	return status == PROCESSING
}

func (fs *flowStatus) start() error {
	if !fs.isStartable() {
		return fmt.Errorf("cannot start because the Flow is in status: %s", fs.status)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.status = PROCESSING
	fs.started = time.Now()
	fs.ended = time.Time{}
	fs.description = ""
	return nil
}

func (fs *flowStatus) cancelling() error {
	if !fs.isCancellable() {
		return fmt.Errorf("cannot cancell because the Flow is in status: %s", fs.status)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
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
	if fs.status == NOT_EXIST {
		fs.started = time.Now()
	}
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
	if !fs.isRestartable() {
		return fmt.Errorf("cannot restart flow, because status is: %v", fs.status)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs = &flowStatus{
		mu:     sync.Mutex{},
		status: WAIT_TO_START,
	}
	return nil
}

func (fs *flowStatus) get() FlowStatus {
	fs.mu.Lock()
	status := FlowStatus{
		fs.status, fs.started, fs.ended,
		fs.description, fs.countRead, fs.countWrite, fs.countMax,
	}
	fs.mu.Unlock()
	return status
}

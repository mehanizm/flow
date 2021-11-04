package flow

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func Test_flowStatus_start(t *testing.T) {
	type fields struct {
		mu sync.Mutex
	}
	tests := []struct {
		name        string
		status      Status
		started     time.Time
		ended       time.Time
		description string
		wantErr     bool
	}{
		{
			name:        "01_positive",
			status:      WAIT_TO_START,
			started:     time.Now(),
			ended:       time.Now(),
			description: "not empty",
			wantErr:     false,
		},
		{
			name:        "01_error",
			status:      CANCELLING,
			started:     time.Now(),
			ended:       time.Now(),
			description: "not empty",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newFlowStatus()
			fs.status = tt.status
			fs.started = tt.started
			fs.ended = tt.ended
			fs.description = tt.description
			err := fs.start()
			if err == nil {
				if !reflect.DeepEqual(fs.ended, time.Time{}) {
					t.Errorf("ended is not empty: %v", fs.ended)
				}
				if fs.description != "" {
					t.Errorf("description is not empty: %v", fs.description)
				}
				if fs.started.Sub(tt.started) > 1*time.Second {
					t.Errorf("started is more than 1 second from now: %v", tt.started)
				}
			} else if !tt.wantErr {
				t.Errorf("flowStatus.start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_flowStatus_cancelling(t *testing.T) {
	t.Run("check cancellable", func(t *testing.T) {
		fs := newFlowStatus()
		fs.status = CANCELLED
		if fs.isCancellable() {
			t.Errorf("should be no cancellable")
		}
		if fs.restart() == nil {
			t.Errorf("should not be restartable")
		}
	})
}

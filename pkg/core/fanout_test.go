package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFanOut_TerminalStatus(t *testing.T) {
	tests := []struct {
		name       string
		fanOut     FanOut
		wantDone   bool
		wantStatus FanOutStatus
	}{
		{
			name: "collect all all cancelled completes",
			fanOut: FanOut{
				TotalCount:     3,
				CancelledCount: 3,
				Strategy:       StrategyCollectAll,
			},
			wantDone:   true,
			wantStatus: FanOutCompleted,
		},
		{
			name: "collect all partial not done",
			fanOut: FanOut{
				TotalCount:     3,
				CompletedCount: 1,
				Strategy:       StrategyCollectAll,
			},
			wantDone: false,
		},
		{
			name: "fail fast failed before all accounted",
			fanOut: FanOut{
				TotalCount:  5,
				FailedCount: 1,
				Strategy:    StrategyFailFast,
			},
			wantDone:   true,
			wantStatus: FanOutFailed,
		},
		{
			name: "fail fast all completed",
			fanOut: FanOut{
				TotalCount:     2,
				CompletedCount: 2,
				Strategy:       StrategyFailFast,
			},
			wantDone:   true,
			wantStatus: FanOutCompleted,
		},
		{
			name: "fail fast cancelled before all accounted",
			fanOut: FanOut{
				TotalCount:     5,
				CancelledCount: 1,
				Strategy:       StrategyFailFast,
			},
			wantDone:   true,
			wantStatus: FanOutFailed,
		},
		{
			name: "threshold 0.8 one failed still winnable",
			fanOut: FanOut{
				TotalCount:  5,
				FailedCount: 1,
				Strategy:    StrategyThreshold,
				Threshold:   0.8,
			},
			wantDone: false,
		},
		{
			name: "threshold 0.8 four completed one failed completes",
			fanOut: FanOut{
				TotalCount:     5,
				CompletedCount: 4,
				FailedCount:    1,
				Strategy:       StrategyThreshold,
				Threshold:      0.8,
			},
			wantDone:   true,
			wantStatus: FanOutCompleted,
		},
		{
			name: "threshold 0.8 three completed two failed fails",
			fanOut: FanOut{
				TotalCount:     5,
				CompletedCount: 3,
				FailedCount:    2,
				Strategy:       StrategyThreshold,
				Threshold:      0.8,
			},
			wantDone:   true,
			wantStatus: FanOutFailed,
		},
		{
			name: "threshold cancelled counts against",
			fanOut: FanOut{
				TotalCount:     10,
				CompletedCount: 7,
				CancelledCount: 3,
				Strategy:       StrategyThreshold,
				Threshold:      0.8,
			},
			wantDone:   true,
			wantStatus: FanOutFailed,
		},
		{
			name: "threshold one single completed",
			fanOut: FanOut{
				TotalCount:     1,
				CompletedCount: 1,
				Strategy:       StrategyThreshold,
				Threshold:      1.0,
			},
			wantDone:   true,
			wantStatus: FanOutCompleted,
		},
		{
			name: "threshold zero all cancelled completes",
			fanOut: FanOut{
				TotalCount:     3,
				CancelledCount: 3,
				Strategy:       StrategyThreshold,
				Threshold:      0.0,
			},
			wantDone:   true,
			wantStatus: FanOutCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done, status := tt.fanOut.TerminalStatus()
			assert.Equal(t, tt.wantDone, done)
			if tt.wantDone {
				assert.Equal(t, tt.wantStatus, status)
			}
		})
	}
}

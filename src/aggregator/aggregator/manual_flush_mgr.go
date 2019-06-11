// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package aggregator

import (
	"fmt"
	"sync"
)

type ManualFlushManager struct {
	mu       *sync.RWMutex
	state    flushManagerState
	flushers map[metricListID]flushingMetricList
}

func NewManualFlushManager() *ManualFlushManager {
	return &ManualFlushManager{
		mu:       &sync.RWMutex{},
		state:    flushManagerNotOpen,
		flushers: make(map[metricListID]flushingMetricList),
	}
}

func (mgr *ManualFlushManager) Reset() error {
	return nil
}

func (mgr *ManualFlushManager) Open() error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.state = flushManagerOpen
	return nil
}

func (mgr *ManualFlushManager) Status() FlushStatus {
	return FlushStatus{
		ElectionState: LeaderState,
		CanLead:       true,
	}
}

func (mgr *ManualFlushManager) Register(flusher flushingMetricList) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.flushers[flusher.ID()] = flusher
	return nil
}

func (mgr *ManualFlushManager) Unregister(flusher flushingMetricList) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	fid := flusher.ID()
	_, ok := mgr.flushers[fid]
	if !ok {
		return fmt.Errorf("no such flusher %+v", fid)
	}
	delete(mgr.flushers, fid)
	return nil
}

func (*ManualFlushManager) Close() error {
	return nil
}

func (mgr *ManualFlushManager) Flush(req flushRequest) error {
	// use a copy of the current flusher list so that we can handle flushes which
	// themselves add metrics (causing deadlock).
	// This can occur when using an aggregator which "forwards" to itself.
	snapshot := mgr.flusherSnapshot()
	i := 0
	for _, flusher := range snapshot {
		flusher.Flush(req)
		i++
	}
	return nil
}

func (mgr *ManualFlushManager) flusherSnapshot() map[metricListID]flushingMetricList {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	snapshot := make(map[metricListID]flushingMetricList, len(mgr.flushers))
	for k, flusher := range mgr.flushers {
		snapshot[k] = flusher
	}
	return snapshot
}

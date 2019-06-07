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

package handler

import (
	"sync"

	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/uber-go/tally"
)

type capturingWriter struct {
	results    *[]aggregated.MetricWithStoragePolicy
	resultLock *sync.Mutex
}

func (w *capturingWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	w.resultLock.Lock()
	var fullID []byte
	fullID = append(fullID, mp.ChunkedID.Prefix...)
	fullID = append(fullID, mp.ChunkedID.Data...)
	fullID = append(fullID, mp.ChunkedID.Suffix...)
	metric := aggregated.Metric{
		ID:        fullID,
		TimeNanos: mp.TimeNanos,
		Value:     mp.Value,
	}
	*w.results = append(*w.results, aggregated.MetricWithStoragePolicy{
		Metric:        metric,
		StoragePolicy: mp.StoragePolicy,
	})
	w.resultLock.Unlock()
	return nil
}

func (w *capturingWriter) Flush() error { return nil }
func (w *capturingWriter) Close() error { return nil }

type CapturingHandler struct {
	results    []aggregated.MetricWithStoragePolicy
	resultLock *sync.Mutex
}

func NewCapturingHandler() *CapturingHandler {
	return &CapturingHandler{
		resultLock: &sync.Mutex{},
	}
}

func (h *CapturingHandler) Results() []aggregated.MetricWithStoragePolicy {
	return h.results
}

func (h *CapturingHandler) NewWriter(tally.Scope) (writer.Writer, error) {
	return &capturingWriter{results: &h.results, resultLock: h.resultLock}, nil
}

func (h *CapturingHandler) Close() {}

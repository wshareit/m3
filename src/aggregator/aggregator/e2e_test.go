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
	"math"
	_ "net/http/pprof"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/collector/reporter/m3aggregator"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/id/m3"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

const (
	rule = `
name: cpu quota rollup per service per cluster
filter: "service:docker class:usi name:cpu_quota docker_service:* cluster:* type:gauge"
targets:
  - pipeline:
    - aggregation: Max  # Last during the resolution win
    - rollup:
        newName: cpu_quota.per.service.per.cluster
        aggregation:    # Sum the metric
          - Sum
        tags:
          - cluster
          - docker_service
    storagePolicies:
      - resolution: 1m
        retention: 40d
      - resolution: 10m
        retention: 3y
`
	testUser = "m3-test-user"

	id_      = `stats.phx3.gauges.m3+cpu_quota+class=usi,cluster=phx3-prod05,dc=phx3,deployment=production,docker_app=hadoop-container,docker_service=hadoop-container,env=production,environment=production,instance=0,job=cexporter_custom,pipe=us1,service=docker,type=gauge,uber_region=phx,uber_zone=phx3`
	rollupID = "m3+cpu_quota.per.service.per.cluster+cluster=phx3-prod05,docker_service=hadoop-container,m3_rollup=true"
)

func TestT2641745(t *testing.T) {
	var proto view.RollupRule
	require.NoError(t, yaml.Unmarshal([]byte(rule), &proto))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	capturingHandler := handler.NewCapturingHandler()
	flusher := NewManualFlushManager()

	start := time.Unix(1559933821, 0)
	sclock := settableClock{now: start}
	clockOpts := clock.NewOptions().SetNowFn(sclock.Now)
	const maxForwardingDelay = 100 * time.Second
	opts := testOptions(ctrl).SetFlushHandler(capturingHandler).
		SetFlushManager(flusher).
		SetClockOptions(clockOpts).
		SetPlacementManager(defaultPlacementManager(t)).
		SetMaxAllowedForwardingDelayFn(func(resolution time.Duration, numForwardedTimes int) time.Duration {
			// default for tests is math.MaxInt64, which messes with some buried logic in
			// forwardedMetricList (src/aggregator/aggregator/list.go:646)
			// that subtracts the forwarding delay, which is later used
			// to determine staleness and whether
			// a metric gets flushed (src/aggregator/aggregator/list.go:351).
			return maxForwardingDelay
		}).
		SetEntryCheckInterval(0)

	fw := NewForwardingWriter(opts.AdminClient())
	opts = opts.SetAdminClient(fw)

	agg := NewAggregator(opts)
	fw.SetAggregator(agg)

	require.NoError(t, agg.Open())

	updateMetadata := rules.NewRuleSetUpdateHelper(0).
		NewUpdateMetadata(0, testUser)

	rs := rules.NewEmptyRuleSet("docker", updateMetadata, m3.NewRulesOptions(
		[]byte(`name`), nil))

	_, err := rs.AddRollupRule(proto, updateMetadata)
	require.NoError(t, err)

	activeSet := rs.ActiveSet(1)

	reporter := m3aggregator.NewReporter(activeRuleSetMatcher{m: activeSet}, NewLocalAggClient(agg), m3aggregator.NewReporterOptions().SetClockOptions(clockOpts))

	dps := map[string][]ts.Datapoint{
		id_: datapointsAtFixedInterval(start, time.Second, []float64{6.0, 5.0}),
	}

	iterPool := id.NewSortedTagIteratorPool(pool.NewObjectPoolOptions())

	// END SETUP

	for seriesID, series := range dps {
		for _, dp := range series {
			sclock.SetNow(dp.Timestamp)
			require.NoError(t,
				reporter.ReportGauge(m3.NewID([]byte(seriesID), iterPool), dp.Value))
		}
	}

	sclock.Advance(10 * time.Second)
	req := flushRequest{
		CutoverNanos: math.MinInt64,
		CutoffNanos:  math.MaxInt64,
	}
	require.NoError(t, flusher.Flush(req))

	// flush all forwarded metrics
	sclock.Advance(maxForwardingDelay)
	require.NoError(t, flusher.Flush(flushRequest{
		CutoverNanos: math.MinInt64,
		CutoffNanos:  math.MaxInt64,
	}))

	assertResultsEqual(t, map[string][]float64{
		rollupID: {6},

		// N.B.: this value fails; it seems like the same data (5.0) is getting
		// flushed twice for some reason.
		id_: {5},
	}, capturingHandler.Results())
}

// Utils

func datapointsAtFixedInterval(start time.Time, resolution time.Duration, values []float64) []ts.Datapoint {
	rtn := make([]ts.Datapoint, 0, len(values))
	for i, val := range values {
		rtn = append(rtn, ts.Datapoint{
			Value:     val,
			Timestamp: start.Add(time.Duration(int64(resolution) * int64(i))),
		})
	}

	return rtn
}

type settableClock struct {
	now time.Time
}

func (sc *settableClock) SetNow(now time.Time) {
	if now.Before(sc.now) {
		panic(fmt.Sprintf("tried to make clock go back in time from %+v to %+v", sc.now, now))
	}
	sc.now = now
}

func (sc *settableClock) Advance(duration time.Duration) {
	sc.now = sc.now.Add(duration)
}

func (sc *settableClock) Now() time.Time {
	return sc.now
}

func assertResultsEqual(t *testing.T, expected map[string][]float64, actual []aggregated.MetricWithStoragePolicy) bool {
	readableActual := make(map[string][]float64)

	for _, a := range actual {
		k := string(a.ID)
		// strip off the prefix again
		k = strings.TrimPrefix(k, "stats.gauges.")
		readableActual[k] = append(readableActual[k], a.Value)
	}

	return assert.Equal(t, expected, readableActual)
}

// ForwardingWriter is a test utility which forwards metric requests directly
// to an in-process aggregator. Everything else goes to the wrapped client.
type ForwardingWriter struct {
	client.AdminClient

	// agg is the target for forwarded metrics. N.B.: this has to be late
	// binding, since there's a circular dependency between the aggregator
	// and the writer.
	agg Aggregator
}

func NewForwardingWriter(baseClient client.AdminClient) *ForwardingWriter {
	return &ForwardingWriter{
		AdminClient: baseClient,
	}
}

func (fw *ForwardingWriter) SetAggregator(agg Aggregator) {
	fw.agg = agg
}

func (fw *ForwardingWriter) WriteForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	return fw.agg.AddForwarded(metric, metadata)
}

type aggClient struct {
	agg Aggregator
}

func NewLocalAggClient(agg Aggregator) *aggClient {
	return &aggClient{agg: agg}
}

func (aggClient) Init() error {
	return nil
}

func (ac *aggClient) WriteUntimedCounter(
	counter unaggregated.Counter,
	metadatas metadata.StagedMetadatas,
) error {
	counter.ToUnion()
	return ac.agg.AddUntimed(counter.ToUnion(), metadatas)
}

func (ac *aggClient) WriteUntimedBatchTimer(
	batchTimer unaggregated.BatchTimer,
	metadatas metadata.StagedMetadatas,
) error {
	return ac.agg.AddUntimed(batchTimer.ToUnion(), metadatas)

}

func (ac *aggClient) WriteUntimedGauge(
	gauge unaggregated.Gauge,
	metadatas metadata.StagedMetadatas,
) error {
	return ac.agg.AddUntimed(gauge.ToUnion(), metadatas)
}

func (ac *aggClient) WriteTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	return ac.agg.AddTimed(metric, metadata)
}

func (aggClient) Flush() error {
	return nil
}

func (aggClient) Close() error {
	return nil
}

// activeRuleSetMatcher implements matcher.Matcher based on a single active
// rule set. This avoids the caching and M3KV layers and allows you to use
// rules more directly as a matcher.
type activeRuleSetMatcher struct {
	m rules.Matcher
}

func (ar activeRuleSetMatcher) ForwardMatch(id id.ID, fromNanos, toNanos int64) rules.MatchResult {
	return ar.m.ForwardMatch(id.Bytes(), fromNanos, toNanos)
}

func (ar activeRuleSetMatcher) Close() error {
	return nil
}

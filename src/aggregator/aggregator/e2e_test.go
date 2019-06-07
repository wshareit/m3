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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id/m3"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/x/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"
)

const rule = `
name: cpu quota rollup per service per cluster
filter: service:docker class:usi name:cpu_quota docker_service:* cluster:* type:gauge
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

var id_ = []byte(`stats.phx3.gauges.m3+cpu_quota+class=usi,cluster=phx3-prod05,dc=phx3,deployment=production,docker_app=hadoop-container,docker_service=hadoop-container,env=production,environment=production,instance=0,job=cexporter_custom,pipe=us1,service=docker,type=gauge,uber_region=phx,uber_zone=phx3`)

func TestE2E(t *testing.T) {
	var proto view.RollupRule
	require.NoError(t, yaml.Unmarshal([]byte(rule), &proto))
	fmt.Println("Adsf")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	capturingHandler := handler.NewCapturingHandler()
	flusher := NewManualFlushManager()

	opts := testOptions(ctrl).SetFlushHandler(capturingHandler).
		SetFlushManager(flusher).
		SetClockOptions(clock.NewOptions().SetNowFn(
			alwaysAdvancingNow(time.Unix(1559933821, 0), 10*time.Second))).
		SetPlacementManager(defaultPlacementManager(t)).
		SetEntryCheckInterval(0)

	agg := NewAggregator(opts)
	require.NoError(t, agg.Open())

	updateMetadata := rules.NewRuleSetUpdateHelper(0).
		NewUpdateMetadata(1, "asdf")

	rs := rules.NewEmptyRuleSet("docker", updateMetadata, m3.NewRulesOptions(
		[]byte(`service`), nil))
	// SetIsRollupIDFn(m3.IsRollupID))

	_, err := rs.AddRollupRule(proto, updateMetadata)
	require.NoError(t, err)

	activeSet := rs.ActiveSet(2)
	matchRes := activeSet.ForwardMatch(id_, 2, math.MaxInt64)
	stagedMetadatas := matchRes.ForExistingIDAt(3)

	require.NotEmpty(t, stagedMetadatas)
	agg.AddUntimed(unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		ID:       id_,
		GaugeVal: 5.0,
	}, stagedMetadatas)

	flusher.Flush()
	assert.NotEmpty(t, capturingHandler.Results())
}

func alwaysAdvancingNow(start time.Time, interval time.Duration) clock.NowFn {
	// Start the clock at start - interval so that the first value we return
	// is actually start.
	startNanos := atomic.NewInt64(start.UnixNano() - interval.Nanoseconds())

	return func() time.Time {
		return time.Unix(0, startNanos.Add(interval.Nanoseconds()))
	}
}

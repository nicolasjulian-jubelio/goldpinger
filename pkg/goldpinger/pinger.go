// Copyright 2018 Bloomberg Finance L.P.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goldpinger

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	apiclient "github.com/bloomberg/goldpinger/v3/pkg/client"
	"github.com/bloomberg/goldpinger/v3/pkg/client/operations"
	"github.com/bloomberg/goldpinger/v3/pkg/models"
	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Pinger contains all the info needed by a goroutine to continuously ping a pod
type Pinger struct {
	pod         *GoldpingerPod
	client      *apiclient.Goldpinger
	timeout     time.Duration
	histogram   prometheus.Observer
	hostIPv4    strfmt.IPv4
	podIPv4     strfmt.IPv4
	resultsChan chan<- PingAllPodsResult
	stopChan    chan struct{}
	logger      *zap.Logger

	// metricsMux serializes the per-peer metric writes of an in-flight Ping
	// with the cleanup in Stop, so a probe that finishes after the peer was
	// removed cannot recreate the label sets Stop deleted.
	metricsMux sync.Mutex
	stopped    bool
}

// NewPinger constructs and returns a Pinger object responsible for pinging a single
// goldpinger pod
func NewPinger(pod *GoldpingerPod, resultsChan chan<- PingAllPodsResult) *Pinger {
	p := Pinger{
		pod:         pod,
		timeout:     GoldpingerConfig.PingTimeout,
		resultsChan: resultsChan,
		stopChan:    make(chan struct{}),

		histogram: goldpingerResponseTimePeersHistogram.WithLabelValues(
			GoldpingerConfig.Hostname,
			"ping",
			pod.HostIP,
			pod.PodIP,
		),

		logger: zap.L().With(
			zap.String("op", "pinger"),
			zap.String("name", pod.Name),
			zap.String("hostIP", pod.HostIP),
			zap.String("podIP", pod.PodIP),
		),
	}

	// Initialize the host/pod IPv4
	p.hostIPv4.UnmarshalText([]byte(pod.HostIP))
	p.podIPv4.UnmarshalText([]byte(pod.PodIP))

	return &p
}

// getClient returns a client that can be used to ping the given pod
// On error, it returns a static result
func (p *Pinger) getClient() (*apiclient.Goldpinger, error) {
	if p.client != nil {
		return p.client, nil
	}

	client, err := getClient(pickPodHostIP(p.pod.PodIP, p.pod.HostIP))
	if err != nil {
		p.logger.Warn("Could not get client", zap.Error(err))
		OK := false
		p.resultsChan <- PingAllPodsResult{
			podName: p.pod.Name,
			podResult: models.PodResult{
				PingTime:       strfmt.DateTime(time.Now()),
				PodIP:          p.podIPv4,
				HostIP:         p.hostIPv4,
				OK:             &OK,
				Error:          err.Error(),
				StatusCode:     500,
				ResponseTimeMs: 0,
			},
		}
		return nil, err
	}
	p.client = client
	return p.client, nil
}

// Ping makes a single ping request to the given pod
func (p *Pinger) Ping() {
	client, err := p.getClient()
	if err != nil {
		return
	}

	CountCall("made", "ping")
	start := time.Now()

	// Run HTTP ping and UDP probe concurrently
	var udpResult UDPProbeResult
	var wg sync.WaitGroup
	if GoldpingerConfig.UDPEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			targetIP := pickPodHostIP(p.pod.PodIP, p.pod.HostIP)
			udpResult = ProbeUDP(
				targetIP,
				GoldpingerConfig.UDPPort,
				GoldpingerConfig.UDPPacketCount,
				GoldpingerConfig.UDPPacketSize,
				GoldpingerConfig.UDPTimeout,
			)
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	params := operations.NewPingParamsWithContext(ctx)
	resp, err := client.Operations.Ping(params)
	responseTime := time.Since(start)
	responseTimeMs := responseTime.Nanoseconds() / int64(time.Millisecond)
	p.histogram.Observe(responseTime.Seconds())

	// Wait for UDP probe to complete
	wg.Wait()

	OK := (err == nil)

	var lossPct float64
	var hopCount int32
	var udpRttMs float64
	if GoldpingerConfig.UDPEnabled {
		lossPct = udpResult.LossPct
		hopCount = udpResult.HopCount
		udpRttMs = udpResult.AvgRttS * 1000.0
		if udpResult.Err != nil {
			p.logger.Warn("UDP probe error", zap.Error(udpResult.Err))
		} else {
			p.logger.Debug("UDP probe complete",
				zap.Float64("lossPct", lossPct),
				zap.Int32("hopCount", hopCount),
				zap.Float64("udpRttMs", udpRttMs),
			)
		}
		p.recordUDPResult(udpResult)
	}

	if OK {
		p.resultsChan <- PingAllPodsResult{
			podName: p.pod.Name,
			podResult: models.PodResult{
				PingTime:       strfmt.DateTime(start),
				PodIP:          p.podIPv4,
				HostIP:         p.hostIPv4,
				OK:             &OK,
				Response:       resp.Payload,
				StatusCode:     200,
				ResponseTimeMs: responseTimeMs,
				LossPct:        lossPct,
				HopCount:       hopCount,
				UDPRttMs:       udpRttMs,
			},
		}
		p.logger.Debug("Success pinging pod", zap.Duration("responseTime", responseTime))
	} else {
		p.resultsChan <- PingAllPodsResult{
			podName: p.pod.Name,
			podResult: models.PodResult{
				PingTime:       strfmt.DateTime(start),
				PodIP:          p.podIPv4,
				HostIP:         p.hostIPv4,
				OK:             &OK,
				Error:          err.Error(),
				StatusCode:     504,
				ResponseTimeMs: responseTimeMs,
				LossPct:        lossPct,
				HopCount:       hopCount,
				UDPRttMs:       udpRttMs,
			},
		}
		p.logger.Warn("Ping returned error", zap.Duration("responseTime", responseTime), zap.Error(err))
		CountError("ping")
	}
}

// recordUDPResult records the per-peer UDP metrics for one probe. It records
// nothing once the pinger has been stopped: the probe may have been in flight
// against a peer that no longer exists, and writing would recreate the label
// sets Stop deleted, leaving them in /metrics with no pinger to update or
// remove them. Returns whether the metrics were recorded.
func (p *Pinger) recordUDPResult(r UDPProbeResult) bool {
	p.metricsMux.Lock()
	defer p.metricsMux.Unlock()
	if p.stopped {
		return false
	}

	SetPeerLossPct(p.pod.HostIP, p.pod.PodIP, r.LossPct)
	SetPeerHopCount(p.pod.HostIP, p.pod.PodIP, r.HopCount)
	if r.AvgRttS > 0 {
		ObservePeerUDPRtt(p.pod.HostIP, p.pod.PodIP, r.AvgRttS)
	}
	if r.Duplicates > 0 {
		CountUDPDuplicates(p.pod.HostIP, p.pod.PodIP, r.Duplicates)
	}
	if r.OutOfOrder > 0 {
		CountUDPOutOfOrder(p.pod.HostIP, p.pod.PodIP, r.OutOfOrder)
	}
	for i := 0; i < r.Errors; i++ {
		CountUDPError(pickPodHostIP(p.pod.PodIP, p.pod.HostIP))
	}
	return true
}

// Stop stops the continuous pinging and removes the peer's metric label sets.
// A Ping already in flight runs to completion but no longer records per-peer
// metrics, so the deleted label sets stay deleted.
func (p *Pinger) Stop() {
	close(p.stopChan)

	p.metricsMux.Lock()
	defer p.metricsMux.Unlock()
	p.stopped = true

	// Clean up stale metric labels for this peer so defunct pod IPs
	// don't linger in /metrics after rolling updates (see #167)
	DeletePeerMetrics(p.pod.HostIP, p.pod.PodIP)
	if GoldpingerConfig.UDPEnabled {
		DeletePeerUDPMetrics(p.pod.HostIP, p.pod.PodIP)
	}
}

// PingContinuously continuously pings the given pod with a delay between
// `period` and `period + jitterFactor * period`
func (p *Pinger) PingContinuously(initialWait time.Duration, period time.Duration, jitterFactor float64) {
	p.logger.Info(
		"Starting pinger",
		zap.Duration("period", period),
		zap.Duration("initialWait", initialWait),
		zap.Float64("jitterFactor", jitterFactor),
	)

	timer := time.NewTimer(initialWait)

	select {
	case <-timer.C:
		wait.JitterUntil(p.Ping, period, jitterFactor, false, p.stopChan)
	case <-p.stopChan:
		// Do nothing
	}
	// We are done, send a message on the results channel to delete this
	p.resultsChan <- PingAllPodsResult{podName: p.pod.Name, deleted: true}
}

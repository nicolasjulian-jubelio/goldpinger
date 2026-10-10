package goldpinger

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// udpPeerCollectors lists every metric the pinger writes per peer from a UDP
// probe result.
func udpPeerCollectors() map[string]prometheus.Collector {
	return map[string]prometheus.Collector{
		"goldpinger_peers_loss_pct":         goldpingerPeersLossPct,
		"goldpinger_peers_hop_count":        goldpingerPeersHopCount,
		"goldpinger_peers_udp_rtt_s":        goldpingerPeersUDPRtt,
		"goldpinger_udp_duplicates_total":   goldpingerUDPDuplicatesCounter,
		"goldpinger_udp_out_of_order_total": goldpingerUDPOutOfOrderCounter,
		"goldpinger_udp_errors_total":       goldpingerUDPErrorsCounter,
	}
}

// withUDPTestConfig sets the configuration a pinger reads and restores it
// when the test ends.
func withUDPTestConfig(t *testing.T) {
	t.Helper()
	origHostname := GoldpingerConfig.Hostname
	origUDPEnabled := GoldpingerConfig.UDPEnabled
	origUseHostIP := GoldpingerConfig.UseHostIP
	GoldpingerConfig.Hostname = "test-instance"
	GoldpingerConfig.UDPEnabled = true
	GoldpingerConfig.UseHostIP = false
	t.Cleanup(func() {
		GoldpingerConfig.Hostname = origHostname
		GoldpingerConfig.UDPEnabled = origUDPEnabled
		GoldpingerConfig.UseHostIP = origUseHostIP
	})
}

// fullUDPResult sets every per-peer UDP metric when recorded.
var fullUDPResult = UDPProbeResult{LossPct: 5, HopCount: 2, AvgRttS: 0.001, Duplicates: 1, OutOfOrder: 1, Errors: 1}

func newTestPinger(hostIP, podIP string) *Pinger {
	pod := &GoldpingerPod{Name: "goldpinger-peer", HostIP: hostIP, PodIP: podIP}
	return NewPinger(pod, make(chan PingAllPodsResult, 1))
}

// TestPingerStop_DropsResultOfInFlightProbe reproduces a probe that is still
// running when its peer is removed: the peer's pod is gone, so the probe
// reports 100% loss, and it finishes after Stop deleted the peer's metrics.
// Recording that result used to recreate the label sets, which then stayed in
// /metrics for the life of the process because no pinger owned them anymore.
func TestPingerStop_DropsResultOfInFlightProbe(t *testing.T) {
	tests := []struct {
		name   string
		hostIP string
		podIP  string
	}{
		{"IPv4", "192.0.2.1", "192.0.2.10"},
		{"IPv6", "2001:db8::1", "2001:db8:1::10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withUDPTestConfig(t)
			p := newTestPinger(tt.hostIP, tt.podIP)

			if !p.recordUDPResult(fullUDPResult) {
				t.Fatal("recordUDPResult before Stop returned false, want true")
			}
			for name, c := range udpPeerCollectors() {
				if countMetrics(c) == 0 {
					t.Fatalf("metric %s has no label values before Stop — test setup is broken", name)
				}
			}

			p.Stop()

			select {
			case <-p.stopChan:
			default:
				t.Error("Stop did not close stopChan")
			}
			for name, c := range udpPeerCollectors() {
				if n := countMetrics(c); n != 0 {
					t.Errorf("metric %s has %d label set(s) after Stop, want 0", name, n)
				}
			}

			// The in-flight probe completes against the removed peer.
			if p.recordUDPResult(UDPProbeResult{LossPct: 100, Errors: 1}) {
				t.Error("recordUDPResult after Stop returned true, want false")
			}
			for name, c := range udpPeerCollectors() {
				if n := countMetrics(c); n != 0 {
					t.Errorf("metric %s has %d label set(s) after an in-flight probe finished, want 0", name, n)
				}
			}
		})
	}
}

// TestPingerStop_ConcurrentWithRecord races Stop against probes finishing.
// Whichever order they run in, no label set may survive. Run with -race.
func TestPingerStop_ConcurrentWithRecord(t *testing.T) {
	withUDPTestConfig(t)

	for i := 0; i < 200; i++ {
		p := newTestPinger("192.0.2.1", "192.0.2.10")
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-p.stopChan:
					// The probe that was in flight when Stop was called.
					p.recordUDPResult(UDPProbeResult{LossPct: 100, Errors: 1})
					return
				default:
					p.recordUDPResult(fullUDPResult)
				}
			}
		}()
		p.Stop()
		<-done

		for name, c := range udpPeerCollectors() {
			if n := countMetrics(c); n != 0 {
				t.Fatalf("iteration %d: metric %s has %d label set(s) after Stop, want 0", i, name, n)
			}
		}
	}
}

// TestDestroyPingers_StopsPinger verifies that removing a peer stops its
// pinger, so later probe results are dropped, and forgets it.
func TestDestroyPingers_StopsPinger(t *testing.T) {
	withUDPTestConfig(t)

	pod := &GoldpingerPod{Name: "goldpinger-peer", HostIP: "192.0.2.1", PodIP: "192.0.2.10"}
	p := NewPinger(pod, make(chan PingAllPodsResult, 1))
	pingers := map[string]*Pinger{pod.Name: p}
	p.recordUDPResult(fullUDPResult)

	destroyPingers(pingers, map[string]*GoldpingerPod{pod.Name: pod})

	if _, ok := pingers[pod.Name]; ok {
		t.Error("destroyPingers left the pinger in the map")
	}
	if p.recordUDPResult(fullUDPResult) {
		t.Error("pinger still records metrics after destroyPingers")
	}
	for name, c := range udpPeerCollectors() {
		if n := countMetrics(c); n != 0 {
			t.Errorf("metric %s has %d label set(s) after destroyPingers, want 0", name, n)
		}
	}
}

// healthsidecar probes a matching-engine instance and exposes Kubernetes-style
// health endpoints plus a few derived metrics.
//
// It checks two things on every probe cycle:
//   - the engine's own HTTP exporter (/readyz, /metrics) answers quickly
//   - the engine is making progress: if gRPC requests are being received but
//     me_operations_total has not advanced for --stall-after, the engine is
//     considered wedged (shard thread stuck) and liveness fails.
//
// Endpoints (default :8081):
//   /livez   200 if the engine answered within --unhealthy-after and is not stalled
//   /readyz  200 if the engine reports ready on its own /readyz
//   /healthz JSON status document
//   /metrics sidecar metrics (probe latency, failures, engine state)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type status struct {
	mu               sync.RWMutex
	LastProbe        time.Time `json:"last_probe"`
	LastSuccess      time.Time `json:"last_success"`
	ProbeLatencyMs   float64   `json:"probe_latency_ms"`
	EngineReady      bool      `json:"engine_ready"`
	EngineReachable  bool      `json:"engine_reachable"`
	Stalled          bool      `json:"stalled"`
	Operations       float64   `json:"operations_total"`
	GRPCRequests     float64   `json:"grpc_requests_total"`
	LastOpsChange    time.Time `json:"last_ops_change"`
	ConsecutiveFails int       `json:"consecutive_failures"`
	Error            string    `json:"error,omitempty"`
}

var (
	probesTotal   atomic.Int64
	probeFailures atomic.Int64
	grpcDialOK    atomic.Bool
)

func parseMetric(body, name string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{shard=\"all\"} ") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
				return v, err == nil
			}
		}
	}
	return 0, false
}

func probeOnce(client *http.Client, base string, grpcAddr string, st *status, stallAfter time.Duration) {
	probesTotal.Add(1)
	start := time.Now()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.LastProbe = start

	fail := func(err error) {
		probeFailures.Add(1)
		st.ConsecutiveFails++
		st.EngineReachable = false
		st.EngineReady = false
		st.Error = err.Error()
	}

	resp, err := client.Get(base + "/metrics")
	if err != nil {
		fail(err)
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	resp.Body.Close()
	if err != nil || resp.StatusCode != 200 {
		fail(fmt.Errorf("metrics status %d err %v", resp.StatusCode, err))
		return
	}
	st.ProbeLatencyMs = float64(time.Since(start).Microseconds()) / 1000
	st.EngineReachable = true
	st.ConsecutiveFails = 0
	st.Error = ""
	st.LastSuccess = time.Now()

	rr, err := client.Get(base + "/readyz")
	if err == nil {
		rr.Body.Close()
		st.EngineReady = rr.StatusCode == 200
	} else {
		st.EngineReady = false
	}

	// gRPC port reachability (TCP connect only; keeps the sidecar protocol-free).
	if grpcAddr != "" {
		c, err := net.DialTimeout("tcp", grpcAddr, 500*time.Millisecond)
		if err == nil {
			c.Close()
			grpcDialOK.Store(true)
		} else {
			grpcDialOK.Store(false)
		}
	}

	ops, _ := parseMetric(string(body), "me_operations_total")
	reqs, _ := parseMetric(string(body), "me_grpc_requests_total")
	if ops != st.Operations || st.LastOpsChange.IsZero() {
		st.LastOpsChange = time.Now()
	}
	// Stalled: requests keep arriving but no engine operation completes.
	st.Stalled = reqs > st.GRPCRequests && ops == st.Operations && time.Since(st.LastOpsChange) > stallAfter
	st.Operations = ops
	st.GRPCRequests = reqs
}

func main() {
	engine := flag.String("engine", envOr("ENGINE_METRICS_URL", "http://127.0.0.1:9100"), "engine HTTP exporter base URL")
	grpcAddr := flag.String("grpc", envOr("ENGINE_GRPC_ADDR", "127.0.0.1:50051"), "engine gRPC host:port (TCP reachability)")
	listen := flag.String("listen", envOr("SIDECAR_LISTEN", ":8081"), "sidecar listen address")
	interval := flag.Duration("interval", 2*time.Second, "probe interval")
	timeout := flag.Duration("timeout", 1*time.Second, "per-probe timeout")
	unhealthyAfter := flag.Duration("unhealthy-after", 10*time.Second, "liveness fails if no successful probe for this long")
	stallAfter := flag.Duration("stall-after", 15*time.Second, "liveness fails if requests arrive but no operation completes for this long")
	flag.Parse()

	client := &http.Client{Timeout: *timeout}
	st := &status{}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		t := time.NewTicker(*interval)
		defer t.Stop()
		probeOnce(client, *engine, *grpcAddr, st, *stallAfter)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				probeOnce(client, *engine, *grpcAddr, st, *stallAfter)
			}
		}
	}()

	live := func() (bool, string) {
		st.mu.RLock()
		defer st.mu.RUnlock()
		if st.LastSuccess.IsZero() {
			if time.Since(st.LastProbe) > *unhealthyAfter && !st.LastProbe.IsZero() {
				return false, "engine never answered"
			}
			return true, "starting"
		}
		if time.Since(st.LastSuccess) > *unhealthyAfter {
			return false, "engine unresponsive: " + st.Error
		}
		if st.Stalled {
			return false, "engine stalled: requests arriving but no operations completing"
		}
		return true, "ok"
	}
	ready := func() (bool, string) {
		st.mu.RLock()
		defer st.mu.RUnlock()
		if !st.EngineReachable {
			return false, "engine unreachable"
		}
		if !st.EngineReady {
			return false, "engine not ready"
		}
		if !grpcDialOK.Load() && *grpcAddr != "" {
			return false, "grpc port not accepting connections"
		}
		return true, "ok"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		ok, msg := live()
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		fmt.Fprintln(w, msg)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ok, msg := ready()
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		fmt.Fprintln(w, msg)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		l, lm := live()
		rd, rm := ready()
		st.mu.RLock()
		doc := map[string]any{"live": l, "live_reason": lm, "ready": rd, "ready_reason": rm, "engine": st}
		b, _ := json.MarshalIndent(doc, "", "  ")
		st.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		if !l || !rd {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		w.Write(b)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		l, _ := live()
		rd, _ := ready()
		st.mu.RLock()
		lat := st.ProbeLatencyMs
		stalled := st.Stalled
		st.mu.RUnlock()
		b := func(v bool) int {
			if v {
				return 1
			}
			return 0
		}
		fmt.Fprintf(w, "# TYPE sidecar_probes_total counter\nsidecar_probes_total %d\n", probesTotal.Load())
		fmt.Fprintf(w, "# TYPE sidecar_probe_failures_total counter\nsidecar_probe_failures_total %d\n", probeFailures.Load())
		fmt.Fprintf(w, "# TYPE sidecar_probe_latency_ms gauge\nsidecar_probe_latency_ms %g\n", lat)
		fmt.Fprintf(w, "# TYPE sidecar_engine_live gauge\nsidecar_engine_live %d\n", b(l))
		fmt.Fprintf(w, "# TYPE sidecar_engine_ready gauge\nsidecar_engine_ready %d\n", b(rd))
		fmt.Fprintf(w, "# TYPE sidecar_engine_stalled gauge\nsidecar_engine_stalled %d\n", b(stalled))
	})

	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		srv.Shutdown(shutdownCtx)
	}()
	log.Printf("healthsidecar listening on %s, probing %s every %s", *listen, *engine, *interval)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

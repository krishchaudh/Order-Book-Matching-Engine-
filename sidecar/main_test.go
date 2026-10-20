package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func fakeEngine(ops *atomic.Int64, reqs *atomic.Int64, ready *atomic.Bool) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "me_operations_total{shard=\"all\"} %d\nme_grpc_requests_total %d\n", ops.Load(), reqs.Load())
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			w.WriteHeader(503)
		}
		fmt.Fprintln(w, "ok")
	})
	return httptest.NewServer(mux)
}

func TestParseMetric(t *testing.T) {
	body := "# c\nme_operations_total{shard=\"0\"} 5\nme_operations_total{shard=\"all\"} 7\nme_grpc_requests_total 9\n"
	if v, ok := parseMetric(body, "me_operations_total"); !ok || v != 7 {
		t.Fatalf("got %v %v", v, ok)
	}
	if v, ok := parseMetric(body, "me_grpc_requests_total"); !ok || v != 9 {
		t.Fatalf("got %v %v", v, ok)
	}
}

func TestProbeHealthyAndStall(t *testing.T) {
	var ops, reqs atomic.Int64
	var ready atomic.Bool
	ready.Store(true)
	srv := fakeEngine(&ops, &reqs, &ready)
	defer srv.Close()
	st := &status{}
	client := &http.Client{Timeout: time.Second}
	stall := 50 * time.Millisecond

	probeOnce(client, srv.URL, "", st, stall)
	if !st.EngineReachable || !st.EngineReady || st.Stalled {
		t.Fatalf("expected healthy: %+v", st)
	}
	// Requests arrive, operations advance: not stalled.
	ops.Add(10)
	reqs.Add(10)
	time.Sleep(2 * stall)
	probeOnce(client, srv.URL, "", st, stall)
	if st.Stalled {
		t.Fatal("should not be stalled while ops advance")
	}
	// Requests arrive but operations frozen for > stall window.
	reqs.Add(10)
	time.Sleep(2 * stall)
	probeOnce(client, srv.URL, "", st, stall)
	if !st.Stalled {
		t.Fatal("expected stall detection")
	}
	// Readiness follows the engine.
	ready.Store(false)
	probeOnce(client, srv.URL, "", st, stall)
	if st.EngineReady {
		t.Fatal("expected not ready")
	}
}

func TestProbeUnreachable(t *testing.T) {
	st := &status{}
	client := &http.Client{Timeout: 200 * time.Millisecond}
	probeOnce(client, "http://127.0.0.1:1", "", st, time.Second)
	if st.EngineReachable || st.ConsecutiveFails != 1 || st.Error == "" {
		t.Fatalf("expected failure: %+v", st)
	}
}

// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestStripNamespaceSubcontext(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"storage-engine.defrag-lwm-pct", "defrag-lwm-pct"},
		{"storage-engine.stop-writes-used-pct", "stop-writes-used-pct"},
		{"storage-engine.device", "device"},
		{"storage-engine.compression", "compression"},
		// Non-subcontext params should pass through unchanged
		{"default-ttl", "default-ttl"},
		{"background-query-max-rps", "background-query-max-rps"},
		// "storage-engine" without the dot should NOT be stripped
		{"storage-engine", "storage-engine"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := stripNamespaceSubcontext(tt.input)
			if got != tt.expected {
				t.Errorf("stripNamespaceSubcontext(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestParseSemicolonKV(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want map[string]string
	}{
		{"empty", "", map[string]string{}},
		{"single", "a=1", map[string]string{"a": "1"}},
		{"multi", "a=1;b=2;c=3", map[string]string{"a": "1", "b": "2", "c": "3"}},
		{"value contains equals", "a=k=v;b=2", map[string]string{"a": "k=v", "b": "2"}},
		{"malformed pairs skipped", "a=1;bogus;b=2", map[string]string{"a": "1", "b": "2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseSemicolonKV(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseSemicolonKV(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseColonKV(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want map[string]string
	}{
		{"empty", "", map[string]string{}},
		{"trailing semicolon stripped", "a=1:b=2;", map[string]string{"a": "1", "b": "2"}},
		{"no trailing semicolon", "a=1:b=2", map[string]string{"a": "1", "b": "2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseColonKV(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseColonKV(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestReduceNodeConfigs(t *testing.T) {
	t.Run("all nodes agree returns shared value, no divergence", func(t *testing.T) {
		nodes := map[string]map[string]string{
			"n1": {"k": "v"},
			"n2": {"k": "v"},
		}
		reduced, div := reduceNodeConfigs(nodes, map[string]string{"k": "v"})
		if reduced["k"] != "v" {
			t.Errorf("reduced[k] = %q, want %q", reduced["k"], "v")
		}
		if len(div) != 0 {
			t.Errorf("expected no divergences, got %v", div)
		}
	})

	t.Run("disagreement picks value differing from prior state", func(t *testing.T) {
		nodes := map[string]map[string]string{
			"n1": {"k": "configured"}, // matches prior state / config
			"n2": {"k": "drifted"},    // diverging — should be chosen
		}
		reduced, div := reduceNodeConfigs(nodes, map[string]string{"k": "configured"})
		if reduced["k"] != "drifted" {
			t.Errorf("reduced[k] = %q, want %q (the value that differs from prior state)", reduced["k"], "drifted")
		}
		if len(div) != 1 || div[0].Key != "k" {
			t.Fatalf("expected one divergence on key %q, got %v", "k", div)
		}
		if div[0].NodeValues["n1"] != "configured" || div[0].NodeValues["n2"] != "drifted" {
			t.Errorf("unexpected per-node values: %v", div[0].NodeValues)
		}
	})

	t.Run("disagreement with no prior state falls back to most common", func(t *testing.T) {
		nodes := map[string]map[string]string{
			"n1": {"k": "a"},
			"n2": {"k": "b"},
			"n3": {"k": "b"},
		}
		reduced, div := reduceNodeConfigs(nodes, nil)
		if reduced["k"] != "b" {
			t.Errorf("reduced[k] = %q, want most common %q", reduced["k"], "b")
		}
		if len(div) != 1 {
			t.Errorf("expected one divergence, got %d", len(div))
		}
	})

	t.Run("missing key on some nodes is not divergence if remaining agree", func(t *testing.T) {
		nodes := map[string]map[string]string{
			"n1": {"k": "v"},
			"n2": {}, // didn't report the key
		}
		reduced, div := reduceNodeConfigs(nodes, nil)
		if reduced["k"] != "v" {
			t.Errorf("reduced[k] = %q, want %q", reduced["k"], "v")
		}
		if len(div) != 0 {
			t.Errorf("expected no divergence when only one node reported, got %v", div)
		}
	})
}

func TestParseBuildVersion(t *testing.T) {
	tests := []struct {
		in              string
		maj, minor, pat int
		wantErr         bool
	}{
		{"8.1.2.4", 8, 1, 2, false},
		{"8.1.2", 8, 1, 2, false},
		{"7.2.0.6", 7, 2, 0, false},
		{"8.1", 0, 0, 0, true},
		{"", 0, 0, 0, true},
		{"not-a-version", 0, 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			maj, minor, pat, err := parseBuildVersion(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseBuildVersion(%q) expected error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseBuildVersion(%q) unexpected error: %v", tt.in, err)
			}
			if maj != tt.maj || minor != tt.minor || pat != tt.pat {
				t.Errorf("parseBuildVersion(%q) = %d.%d.%d, want %d.%d.%d", tt.in, maj, minor, pat, tt.maj, tt.minor, tt.pat)
			}
		})
	}
}

func TestVersionAtLeast(t *testing.T) {
	tests := []struct {
		name                      string
		maj, minor, pat           int
		wantMaj, wantMin, wantPat int
		want                      bool
	}{
		{"equal 8.1.2", 8, 1, 2, 8, 1, 2, true},
		{"patch ahead", 8, 1, 3, 8, 1, 2, true},
		{"build 8.1.2.4 uses patch 2", 8, 1, 2, 8, 1, 2, true},
		{"8.1.1 below", 8, 1, 1, 8, 1, 2, false},
		{"8.2.0 above", 8, 2, 0, 8, 1, 2, true},
		{"9.0.0 above", 9, 0, 0, 8, 1, 2, true},
		{"7.2.0 below", 7, 2, 0, 8, 1, 2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := versionAtLeast(tt.maj, tt.minor, tt.pat, tt.wantMaj, tt.wantMin, tt.wantPat)
			if got != tt.want {
				t.Errorf("versionAtLeast(%d.%d.%d, %d.%d.%d) = %v, want %v",
					tt.maj, tt.minor, tt.pat, tt.wantMaj, tt.wantMin, tt.wantPat, got, tt.want)
			}
		})
	}
}

func TestIsSetIndex(t *testing.T) {
	if !isSetIndex(sindexEntry{IndexType: "set"}) {
		t.Error("indextype=set should be a set index")
	}
	if !isSetIndex(sindexEntry{Mode: "digest"}) {
		t.Error("mode=digest should be a set index")
	}
	if isSetIndex(sindexEntry{IndexType: "default", Mode: "secondary"}) {
		t.Error("secondary default index should not be a set index")
	}
}

func TestParseSindexList(t *testing.T) {
	t.Run("empty and ok", func(t *testing.T) {
		if got := parseSindexList(""); len(got) != 0 {
			t.Errorf("empty: got %v", got)
		}
		if got := parseSindexList("ok"); len(got) != 0 {
			t.Errorf("ok: got %v", got)
		}
	})

	t.Run("secondary indexes", func(t *testing.T) {
		raw := "ns=test:indexname=dnr-age-idx:set=donor:bin=age:type=numeric:indextype=default:context=null:exp=null:state=RW"
		got := parseSindexList(raw)
		if len(got) != 1 {
			t.Fatalf("len=%d, want 1", len(got))
		}
		if got[0].Name != "dnr-age-idx" || got[0].Set != "donor" || got[0].IndexType != "default" {
			t.Errorf("got %+v", got[0])
		}
		if isSetIndex(got[0]) {
			t.Error("secondary index parsed as set index")
		}
	})

	t.Run("set index with mode digest", func(t *testing.T) {
		raw := "ns=aerospike:indexname=jobs-idx:set=shuttlex_jobs:bin=null:type=null:indextype=set:mode=digest:state=RW"
		got := parseSindexList(raw)
		if len(got) != 1 {
			t.Fatalf("len=%d, want 1", len(got))
		}
		if !isSetIndex(got[0]) {
			t.Errorf("expected set index, got %+v", got[0])
		}
		if got[0].Namespace != "aerospike" || got[0].Set != "shuttlex_jobs" || got[0].Name != "jobs-idx" {
			t.Errorf("got %+v", got[0])
		}
	})

	t.Run("mixed set and secondary", func(t *testing.T) {
		raw := "ns=test:indexname=age-idx:set=donor:bin=age:indextype=default:state=RW;ns=test:indexname=demo-idx:set=demo:indextype=set:mode=digest:state=RW"
		got := parseSindexList(raw)
		if len(got) != 2 {
			t.Fatalf("len=%d, want 2: %+v", len(got), got)
		}
		var setCount int
		for _, e := range got {
			if isSetIndex(e) {
				setCount++
				if e.Set != "demo" || e.Name != "demo-idx" {
					t.Errorf("set index %+v", e)
				}
			}
		}
		if setCount != 1 {
			t.Errorf("setCount=%d, want 1", setCount)
		}
	})
}

func TestParseSindexImportID(t *testing.T) {
	ns, set, name, err := parseSindexImportID("aerospike/shuttlex_jobs/jobs-idx")
	if err != nil {
		t.Fatal(err)
	}
	if ns != "aerospike" || set != "shuttlex_jobs" || name != "jobs-idx" {
		t.Errorf("got %q %q %q", ns, set, name)
	}
	if _, _, _, err := parseSindexImportID("aerospike/onlytwo"); err == nil {
		t.Error("expected error for two-part id")
	}
	if _, _, _, err := parseSindexImportID("aerospike//name"); err == nil {
		t.Error("expected error for empty set")
	}
}

func TestSindexID(t *testing.T) {
	if got := sindexID("ns", "set", "idx"); got != "ns/set/idx" {
		t.Errorf("got %q", got)
	}
}

func TestRefuseSindexDelete(t *testing.T) {
	setIdx := sindexEntry{IndexType: "set", Mode: "digest", Set: "jobs", Name: "jobs-idx"}
	if err := refuseSindexDelete(setIdx, "aerospike", "jobs", "jobs-idx"); err != nil {
		t.Fatalf("set index on matching set should be allowed: %v", err)
	}

	binIdx := sindexEntry{IndexType: "default", Mode: "secondary", Set: "jobs", Name: "jobs-idx"}
	err := refuseSindexDelete(binIdx, "aerospike", "jobs", "jobs-idx")
	if err == nil {
		t.Fatal("expected error refusing to delete a bin/secondary index")
	}
	if !strings.Contains(err.Error(), "not a set index") {
		t.Fatalf("expected not-a-set-index error, got: %s", err)
	}

	otherSet := sindexEntry{IndexType: "set", Set: "other", Name: "jobs-idx"}
	err = refuseSindexDelete(otherSet, "aerospike", "jobs", "jobs-idx")
	if err == nil {
		t.Fatal("expected error refusing to delete a set index on a different set")
	}
	if !strings.Contains(err.Error(), "belongs to set") {
		t.Fatalf("expected set-mismatch error, got: %s", err)
	}
}

func TestIsSindexPrivilegeError(t *testing.T) {
	if isSindexPrivilegeError(nil) {
		t.Error("nil should not be privilege error")
	}
	if !isSindexPrivilegeError(errors.New("error in asinfo request: sindex-create response: ERROR:81:role violation")) {
		t.Error("role violation should match")
	}
	if isSindexPrivilegeError(errors.New("error in asinfo request: sindex-create response: ERROR:4:invalid state")) {
		t.Error("unrelated error should not match")
	}
}

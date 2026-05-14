// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"reflect"
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

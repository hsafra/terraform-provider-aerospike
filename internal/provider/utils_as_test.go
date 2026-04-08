// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import "testing"

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

// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
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

func TestBuildAtLeast(t *testing.T) {
	tests := []struct {
		name    string
		build   string
		want    bool
		wantErr bool
	}{
		{"equal 8.1.2", "8.1.2", true, false},
		{"four-segment 8.1.2.4", "8.1.2.4", true, false},
		{"patch ahead", "8.1.3", true, false},
		{"prerelease 8.1.2-rc1 below", "8.1.2-rc1", false, false},
		{"four-segment prerelease parses", "8.1.2.0-rc1", true, false},
		{"8.1.1 below", "8.1.1", false, false},
		{"8.2.0 above", "8.2.0", true, false},
		{"9.0.0 above", "9.0.0", true, false},
		{"7.2.0 below", "7.2.0", false, false},
		{"7.2.0.6 below", "7.2.0.6", false, false},
		{"empty", "", false, true},
		{"not-a-version", "not-a-version", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildAtLeast(tt.build, minSetSindexVersion)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("buildAtLeast(%q) expected error", tt.build)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildAtLeast(%q) unexpected error: %v", tt.build, err)
			}
			if got != tt.want {
				t.Errorf("buildAtLeast(%q, 8.1.2) = %v, want %v", tt.build, got, tt.want)
			}
		})
	}
}

func TestMinBuildAtLeast(t *testing.T) {
	tests := []struct {
		name    string
		builds  []string
		want    bool
		wantErr bool
	}{
		{"all at 8.1.2", []string{"8.1.2", "8.1.2.4"}, true, false},
		{"mixed rolling upgrade", []string{"8.1.1", "8.1.2.4"}, false, false},
		{"all below", []string{"7.2.0.6", "8.1.1"}, false, false},
		{"single node 8.1.2", []string{"8.1.2.4"}, true, false},
		{"prerelease is the min", []string{"8.1.2-rc1", "8.1.2"}, false, false},
		{"empty list", nil, false, true},
		{"invalid node", []string{"8.1.2", "not-a-version"}, false, true},
		{"empty string node", []string{"8.1.2", ""}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := minBuildAtLeast(tt.builds, minSetSindexVersion)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("minBuildAtLeast(%v) expected error", tt.builds)
				}
				return
			}
			if err != nil {
				t.Fatalf("minBuildAtLeast(%v) unexpected error: %v", tt.builds, err)
			}
			if got != tt.want {
				t.Errorf("minBuildAtLeast(%v, 8.1.2) = %v, want %v", tt.builds, got, tt.want)
			}
		})
	}
}

func TestServerSupportsSetSindexMemoized(t *testing.T) {
	ok, err := serverSupportsSetSindex(nil)
	if err == nil || ok {
		t.Fatalf("nil conn: got ok=%v err=%v, want error", ok, err)
	}

	conn := &asConnection{}
	ok, err = serverSupportsSetSindex(conn)
	if err == nil || ok {
		t.Fatalf("nil client: got ok=%v err=%v, want error", ok, err)
	}

	// Cached success is returned without a client round-trip.
	conn.setSindexCached = true
	conn.setSindexOK = true
	ok, err = serverSupportsSetSindex(conn)
	if err != nil {
		t.Fatalf("cached true: unexpected error: %v", err)
	}
	if !ok {
		t.Error("cached true: got false")
	}
}

func TestSindexExistsConsensus(t *testing.T) {
	tests := []struct {
		name     string
		exists   []bool
		allTrue  bool
		allFalse bool
		wantErr  bool
	}{
		{"all true", []bool{true, true, true}, true, false, false},
		{"all false", []bool{false, false}, false, true, false},
		{"mixed lag", []bool{true, false, true}, false, false, false},
		{"single true", []bool{true}, true, false, false},
		{"single false", []bool{false}, false, true, false},
		{"empty", nil, false, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allTrue, allFalse, err := sindexExistsConsensus(tt.exists)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("sindexExistsConsensus(%v) expected error", tt.exists)
				}
				return
			}
			if err != nil {
				t.Fatalf("sindexExistsConsensus(%v) unexpected error: %v", tt.exists, err)
			}
			if allTrue != tt.allTrue || allFalse != tt.allFalse {
				t.Errorf("sindexExistsConsensus(%v) = (%v, %v), want (%v, %v)",
					tt.exists, allTrue, allFalse, tt.allTrue, tt.allFalse)
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
		raw := "ns=aerospike:indexname=set1-idx:set=set1:bin=null:type=null:indextype=set:mode=digest:state=RW"
		got := parseSindexList(raw)
		if len(got) != 1 {
			t.Fatalf("len=%d, want 1", len(got))
		}
		if !isSetIndex(got[0]) {
			t.Errorf("expected set index, got %+v", got[0])
		}
		if got[0].Namespace != "aerospike" || got[0].Set != "set1" || got[0].Name != "set1-idx" {
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
	ns, set, name, err := parseSindexImportID("aerospike/set1/set1-idx")
	if err != nil {
		t.Fatal(err)
	}
	if ns != "aerospike" || set != "set1" || name != "set1-idx" {
		t.Errorf("got %q %q %q", ns, set, name)
	}
	if _, _, _, err := parseSindexImportID("aerospike/onlytwo"); err == nil {
		t.Error("expected error for two-part id")
	}
	if _, _, _, err := parseSindexImportID("aerospike//name"); err == nil {
		t.Error("expected error for empty set")
	}
	if _, _, _, err := parseSindexImportID("aerospike/set1/jobs/v2"); err == nil {
		t.Error("expected error for extra slash")
	}
	if _, _, _, err := parseSindexImportID("aerospike/set;x/idx"); err == nil {
		t.Error("expected error for forbidden char in set")
	}
}

func TestCheckSindexIdent(t *testing.T) {
	if err := checkSindexIdent("set1-idx", asSindexNameMaxLen, "index name"); err != nil {
		t.Fatalf("valid name: %v", err)
	}
	if err := checkSindexIdent(strings.Repeat("n", asNamespaceMaxLen), asNamespaceMaxLen, "namespace"); err != nil {
		t.Fatalf("max-length namespace: %v", err)
	}

	tests := []struct {
		s      string
		maxLen int
		sub    string
	}{
		{"", asSindexNameMaxLen, "must not be empty"},
		{strings.Repeat("n", asNamespaceMaxLen+1), asNamespaceMaxLen, "at most 31"},
		{strings.Repeat("s", asSetNameMaxLen+1), asSetNameMaxLen, "at most 63"},
		{"jobs/v2", asSindexNameMaxLen, "/"},
		{"a:b", asSindexNameMaxLen, ":"},
		{"a;b", asSindexNameMaxLen, ";"},
		{"x;indexname=evil", asSetNameMaxLen, ";"},
		{"a=b", asSindexNameMaxLen, "="},
		{"a|b", asSindexNameMaxLen, "|"},
	}
	for _, tt := range tests {
		err := checkSindexIdent(tt.s, tt.maxLen, "ident")
		if err == nil {
			t.Errorf("checkSindexIdent(%q) expected error", tt.s)
			continue
		}
		if !strings.Contains(err.Error(), tt.sub) {
			t.Errorf("checkSindexIdent(%q) = %v, want substring %q", tt.s, err, tt.sub)
		}
	}
}

func TestSindexID(t *testing.T) {
	if got := sindexID("ns", "set", "idx"); got != "ns/set/idx" {
		t.Errorf("got %q", got)
	}
}

func TestRefuseSindexCreateRename(t *testing.T) {
	if err := refuseSindexCreateRename(nil, "aerospike", "set1", "new-idx"); err != nil {
		t.Fatalf("nil existing: %v", err)
	}
	same := &sindexEntry{Name: "set1-idx"}
	if err := refuseSindexCreateRename(same, "aerospike", "set1", "set1-idx"); err != nil {
		t.Fatalf("same name: %v", err)
	}
	other := &sindexEntry{Name: "old-idx"}
	err := refuseSindexCreateRename(other, "aerospike", "set1", "new-idx")
	if err == nil {
		t.Fatal("expected error for differently-named SMD index")
	}
	if !strings.Contains(err.Error(), "old-idx") {
		t.Fatalf("error should name the existing index, got: %s", err)
	}
}

func TestRefuseSindexDelete(t *testing.T) {
	setIdx := sindexEntry{IndexType: "set", Mode: "digest", Set: "set1", Name: "set1-idx"}
	if err := refuseSindexDelete(setIdx, "aerospike", "set1", "set1-idx"); err != nil {
		t.Fatalf("set index on matching set should be allowed: %v", err)
	}

	binIdx := sindexEntry{IndexType: "default", Mode: "secondary", Set: "set1", Name: "set1-idx"}
	err := refuseSindexDelete(binIdx, "aerospike", "set1", "set1-idx")
	if err == nil {
		t.Fatal("expected error refusing to delete a bin/secondary index")
	}
	if !strings.Contains(err.Error(), "not a set index") {
		t.Fatalf("expected not-a-set-index error, got: %s", err)
	}

	otherSet := sindexEntry{IndexType: "set", Set: "other", Name: "set1-idx"}
	err = refuseSindexDelete(otherSet, "aerospike", "set1", "set1-idx")
	if err == nil {
		t.Fatal("expected error refusing to delete a set index on a different set")
	}
	if !strings.Contains(err.Error(), "belongs to set") {
		t.Fatalf("expected set-mismatch error, got: %s", err)
	}
}

func TestIsPrivError(t *testing.T) {
	if isPrivError(nil) {
		t.Error("nil should not be privilege error")
	}
	if !isPrivError(errors.New("error in asinfo request: sindex-create response: ERROR:81:role violation")) {
		t.Error("role violation should match")
	}
	if isPrivError(errors.New("error in asinfo request: sindex-create response: ERROR:4:invalid state")) {
		t.Error("unrelated error should not match")
	}
}

func TestSkipEnableIndex(t *testing.T) {
	skip, diags := skipEnableIndex("aerospike", "set1", "false", true, true)
	if !skip || !diags.HasError() {
		t.Fatal("expected error when disabling an SMD-owned set index")
	}
	found := false
	for _, d := range diags {
		if strings.Contains(d.Detail(), "depends_on") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("error should mention depends_on, got: %v", diags)
	}
}

func setConfigMap(sets map[string]map[string]string) types.Map {
	innerT := types.MapType{ElemType: types.StringType}
	elems := make(map[string]attr.Value, len(sets))
	for setName, params := range sets {
		inner := make(map[string]attr.Value, len(params))
		for k, v := range params {
			inner[k] = types.StringValue(v)
		}
		elems[setName] = types.MapValueMust(types.StringType, inner)
	}
	return types.MapValueMust(innerT, elems)
}

func TestNestedStringMapFromTypesMap(t *testing.T) {
	innerT := types.MapType{ElemType: types.StringType}
	got := nestedStringMapFromTypesMap(setConfigMap(map[string]map[string]string{
		"set1": {"enable-index": "true", "default-ttl": "0"},
	}))
	want := map[string]map[string]string{
		"set1": {"enable-index": "true", "default-ttl": "0"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
	if len(nestedStringMapFromTypesMap(types.MapUnknown(innerT))) != 0 {
		t.Error("unknown should be empty")
	}
	if len(nestedStringMapFromTypesMap(types.MapNull(innerT))) != 0 {
		t.Error("null should be empty")
	}
}

func TestSetConfigHasParam(t *testing.T) {
	m := setConfigMap(map[string]map[string]string{
		"set1": {"default-ttl": "0"},
		"set2": {"enable-index": "true"},
	})
	if !setConfigHasParam(m, "enable-index") {
		t.Error("expected enable-index present")
	}
	if setConfigHasParam(m, "missing") {
		t.Error("expected missing key absent")
	}
}

func TestWarnRemovedSetConfig(t *testing.T) {
	innerT := types.MapType{ElemType: types.StringType}
	state := setConfigMap(map[string]map[string]string{
		"set1": {"enable-index": "true", "default-ttl": "0"},
		"set2": {"default-ttl": "1"},
	})

	t.Run("unknown plan", func(t *testing.T) {
		var diags diag.Diagnostics
		warnRemovedSetConfig(&diags, state, types.MapUnknown(innerT), "aerospike")
		if len(diags) != 0 {
			t.Fatalf("unknown plan should not warn, got %v", diags)
		}
	})

	t.Run("null state", func(t *testing.T) {
		var diags diag.Diagnostics
		warnRemovedSetConfig(&diags, types.MapNull(innerT), types.MapNull(innerT), "aerospike")
		if len(diags) != 0 {
			t.Fatalf("null state should not warn, got %v", diags)
		}
	})

	t.Run("removed set", func(t *testing.T) {
		var diags diag.Diagnostics
		plan := setConfigMap(map[string]map[string]string{
			"set1": {"enable-index": "true", "default-ttl": "0"},
		})
		warnRemovedSetConfig(&diags, state, plan, "aerospike")
		if len(diags) != 1 {
			t.Fatalf("got %d diags, want 1: %v", len(diags), diags)
		}
		if !strings.Contains(diags[0].Detail(), "set2") {
			t.Fatalf("expected set2 removed, got: %s", diags[0].Detail())
		}
	})

	t.Run("removed key", func(t *testing.T) {
		var diags diag.Diagnostics
		plan := setConfigMap(map[string]map[string]string{
			"set1": {"enable-index": "true"},
			"set2": {"default-ttl": "1"},
		})
		warnRemovedSetConfig(&diags, state, plan, "aerospike")
		if len(diags) != 1 {
			t.Fatalf("got %d diags, want 1: %v", len(diags), diags)
		}
		if !strings.Contains(diags[0].Detail(), "default-ttl") {
			t.Fatalf("expected default-ttl removed, got: %s", diags[0].Detail())
		}
	})
}

// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"reflect"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/types"
)

const testEnvKey = "AEROSPIKE_PROVIDER_TEST_OVERRIDE"

func TestWithEnvironmentOverrideString(t *testing.T) {
	t.Run("unset keeps config value", func(t *testing.T) {
		if got := withEnvironmentOverrideString("cfg", testEnvKey); got != "cfg" {
			t.Errorf("got %q, want %q", got, "cfg")
		}
	})
	t.Run("set overrides config value", func(t *testing.T) {
		t.Setenv(testEnvKey, "env")
		if got := withEnvironmentOverrideString("cfg", testEnvKey); got != "env" {
			t.Errorf("got %q, want %q", got, "env")
		}
	})
	t.Run("set to empty still overrides", func(t *testing.T) {
		t.Setenv(testEnvKey, "")
		if got := withEnvironmentOverrideString("cfg", testEnvKey); got != "" {
			t.Errorf("got %q, want empty", got)
		}
	})
}

func TestWithEnvironmentOverrideInt64(t *testing.T) {
	tests := []struct {
		name  string
		env   *string
		want  int64
		input int64
	}{
		{"unset keeps config value", nil, 3000, 3000},
		{"valid number overrides", ptr("4000"), 4000, 3000},
		{"unparseable falls back to config", ptr("abc"), 3000, 3000},
		{"empty falls back to config", ptr(""), 3000, 3000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.env != nil {
				t.Setenv(testEnvKey, *tt.env)
			}
			if got := withEnvironmentOverrideInt64(tt.input, testEnvKey); got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestWithEnvironmentOverrideBool(t *testing.T) {
	tests := []struct {
		name  string
		env   *string
		input bool
		want  bool
	}{
		{"unset keeps config value", nil, true, true},
		{"true overrides false", ptr("true"), false, true},
		{"false overrides true", ptr("false"), true, false},
		{"1 parses as true", ptr("1"), false, true},
		{"unparseable falls back to config", ptr("yes-please"), true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.env != nil {
				t.Setenv(testEnvKey, *tt.env)
			}
			if got := withEnvironmentOverrideBool(tt.input, testEnvKey); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrderLikePrior(t *testing.T) {
	tests := []struct {
		name   string
		prior  []string
		server []string
		want   []string
	}{
		{"keeps prior order", []string{"write", "read"}, []string{"read", "write"}, []string{"write", "read"}},
		{"appends values new on server", []string{"b"}, []string{"a", "b", "c"}, []string{"b", "a", "c"}},
		{"drops values gone from server", []string{"a", "b", "c"}, []string{"c", "a"}, []string{"a", "c"}},
		{"no prior uses server order", nil, []string{"z", "a"}, []string{"z", "a"}},
		{"duplicate prior values collapse", []string{"a", "a"}, []string{"a"}, []string{"a"}},
		{"empty server", []string{"a"}, nil, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prior := make([]types.String, 0, len(tt.prior))
			for _, p := range tt.prior {
				prior = append(prior, types.StringValue(p))
			}
			got := make([]string, 0)
			for _, v := range orderLikePrior(prior, tt.server) {
				got = append(got, v.ValueString())
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("orderLikePrior(%v, %v) = %v, want %v", tt.prior, tt.server, got, tt.want)
			}
		})
	}
}

func ptr(s string) *string { return &s }

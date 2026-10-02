// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/knownvalue"
)

// testAccCheckStringList checks that a list attribute in state equals want, in order.
func testAccCheckStringList(resourceName, attrName string, want []string) resource.TestCheckFunc {
	checks := []resource.TestCheckFunc{
		resource.TestCheckResourceAttr(resourceName, attrName+".#", strconv.Itoa(len(want))),
	}
	for i, w := range want {
		checks = append(checks, resource.TestCheckResourceAttr(resourceName, fmt.Sprintf("%s.%d", attrName, i), w))
	}
	return resource.ComposeAggregateTestCheckFunc(checks...)
}

// knownStringList is a plan check value matching a list equal to want, in order.
func knownStringList(want []string) knownvalue.Check {
	checks := make([]knownvalue.Check, len(want))
	for i, w := range want {
		checks[i] = knownvalue.StringExact(w)
	}
	return knownvalue.ListExact(checks)
}

func TestServiceInfoCommands(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
		want   []string
	}{
		{"nil", nil, []string{}},
		{"empty", map[string]string{}, []string{}},
		{
			"sorted by key",
			map[string]string{"proto-fd-max": "25000", "batch-index-threads": "8", "proto-fd-idle-ms": "60000"},
			[]string{
				"set-config:context=service;batch-index-threads=8",
				"set-config:context=service;proto-fd-idle-ms=60000",
				"set-config:context=service;proto-fd-max=25000",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := serviceInfoCommands(tt.params); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serviceInfoCommands() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceInfoCommands(t *testing.T) {
	tests := []struct {
		name      string
		params    map[string]string
		setConfig map[string]map[string]string
		skip      map[string]bool
		want      []string
	}{
		{"empty", nil, nil, nil, []string{}},
		{
			"namespace params sorted, storage-engine prefix stripped",
			map[string]string{"storage-engine.defrag-lwm-pct": "50", "default-ttl": "0", "nsup-period": "120"},
			nil, nil,
			[]string{
				"set-config:context=namespace;id=test;default-ttl=0",
				"set-config:context=namespace;id=test;nsup-period=120",
				"set-config:context=namespace;id=test;defrag-lwm-pct=50",
			},
		},
		{
			"namespace params before sets, sets and their keys sorted",
			map[string]string{"default-ttl": "0"},
			map[string]map[string]string{
				"zeta":  {"stop-writes-count": "10", "disable-eviction": "true"},
				"alpha": {"default-ttl": "3600"},
			},
			nil,
			[]string{
				"set-config:context=namespace;id=test;default-ttl=0",
				"set-config:context=namespace;id=test;set=alpha;default-ttl=3600",
				"set-config:context=namespace;id=test;set=zeta;disable-eviction=true",
				"set-config:context=namespace;id=test;set=zeta;stop-writes-count=10",
			},
		},
		{
			"enable-index kept when not skipped",
			nil,
			map[string]map[string]string{"s1": {"enable-index": "true"}},
			nil,
			[]string{"set-config:context=namespace;id=test;set=s1;enable-index=true"},
		},
		{
			"enable-index omitted only for skipped sets",
			nil,
			map[string]map[string]string{
				"owned": {"enable-index": "true", "default-ttl": "60"},
				"plain": {"enable-index": "true"},
			},
			map[string]bool{"owned": true},
			[]string{
				"set-config:context=namespace;id=test;set=owned;default-ttl=60",
				"set-config:context=namespace;id=test;set=plain;enable-index=true",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := namespaceInfoCommands("test", tt.params, tt.setConfig, tt.skip); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("namespaceInfoCommands() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSindexInfoCommands(t *testing.T) {
	want := []string{"sindex-create:namespace=test;set=users;indexname=users-idx;indextype=set"}
	if got := sindexInfoCommands("test", "users", "users-idx"); !reflect.DeepEqual(got, want) {
		t.Errorf("sindexInfoCommands() = %v, want %v", got, want)
	}
}

func TestInfoCommandsListNilIsEmpty(t *testing.T) {
	list, diags := infoCommandsList(context.Background(), nil)
	if diags.HasError() {
		t.Fatalf("unexpected diags: %v", diags)
	}
	if list.IsNull() || len(list.Elements()) != 0 {
		t.Errorf("expected empty non-null list, got %v", list)
	}
}

func TestMapFullyKnown(t *testing.T) {
	innerT := types.MapType{ElemType: types.StringType}
	known := types.MapValueMust(types.StringType, map[string]attr.Value{"a": types.StringValue("1")})
	unknownElem := types.MapValueMust(types.StringType, map[string]attr.Value{"a": types.StringUnknown()})

	tests := []struct {
		name string
		m    types.Map
		want bool
	}{
		{"null", types.MapNull(types.StringType), true},
		{"unknown", types.MapUnknown(types.StringType), false},
		{"known", known, true},
		{"unknown element", unknownElem, false},
		{"nested known", types.MapValueMust(innerT, map[string]attr.Value{"s": known}), true},
		{"nested unknown map", types.MapValueMust(innerT, map[string]attr.Value{"s": types.MapUnknown(types.StringType)}), false},
		{"nested unknown element", types.MapValueMust(innerT, map[string]attr.Value{"s": unknownElem}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapFullyKnown(tt.m); got != tt.want {
				t.Errorf("mapFullyKnown() = %v, want %v", got, tt.want)
			}
		})
	}
}

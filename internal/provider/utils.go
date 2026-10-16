// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"os"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework/types"
)

func withEnvironmentOverrideString(currentValue, envOverrideKey string) string {
	envValue, ok := os.LookupEnv(envOverrideKey)
	if ok {
		return envValue
	}

	return currentValue
}

func withEnvironmentOverrideInt64(currentValue int64, envOverrideKey string) int64 {
	envValue, ok := os.LookupEnv(envOverrideKey)
	if ok {
		n, err := strconv.Atoi(envValue)
		if err == nil {
			return (int64(n))
		}
	}

	return currentValue
}

func withEnvironmentOverrideBool(currentValue bool, envOverrideKey string) bool {
	envValue, ok := os.LookupEnv(envOverrideKey)
	if ok {
		b, err := strconv.ParseBool(envValue)
		if err == nil {
			return b
		}
	}

	return currentValue
}

// orderLikePrior returns the server's values ordered like prior: values that
// were in prior keep their relative position, values new on the server are
// appended in server order, and values gone from the server are dropped.
// Aerospike does not preserve the order of list-like settings (user roles,
// role whitelists), so using the server order directly causes perpetual diffs.
func orderLikePrior(prior []types.String, server []string) []types.String {
	onServer := make(map[string]bool, len(server))
	for _, v := range server {
		onServer[v] = true
	}

	out := make([]types.String, 0, len(server))
	seen := make(map[string]bool, len(server))
	for _, p := range prior {
		v := p.ValueString()
		if onServer[v] && !seen[v] {
			out = append(out, types.StringValue(v))
			seen[v] = true
		}
	}
	for _, v := range server {
		if !seen[v] {
			out = append(out, types.StringValue(v))
			seen[v] = true
		}
	}
	return out
}

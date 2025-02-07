// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
	"os"
	"strconv"
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

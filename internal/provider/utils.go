// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"os"
	"strconv"
)

func withEnvironmentOverrideString(currentValue, envOverrideKey string) string {
	if envValue, ok := os.LookupEnv(envOverrideKey); ok {
		return envValue
	}

	return currentValue
}

func withEnvironmentOverrideInt64(currentValue int64, envOverrideKey string) int64 {
	if envValue, ok := os.LookupEnv(envOverrideKey); ok {
		n, err := strconv.Atoi(envValue)
		if err == nil {
			return (int64(n))
		}
	}

	return currentValue
}

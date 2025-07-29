// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
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

func appendStringToListString(str string, list types.List) (types.List, diag.Diagnostics) {
	tempCmds := append(list.Elements(), types.StringValue(str))
	tmpList, diags := types.ListValue(types.StringType, tempCmds)
	return tmpList, diags
}

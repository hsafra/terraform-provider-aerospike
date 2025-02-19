// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

type asCapabilities int

const (
	SetLevelTTL asCapabilities = 7
)

func namespaceExists(conn as.ClientIfc, namespace string) bool {
	key, _ := as.NewKey(namespace, "dummy", "dummy")

	_, err := conn.Get(nil, key)

	return !err.Matches(astypes.INVALID_NAMESPACE)
}

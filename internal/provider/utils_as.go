// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"errors"
	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
	"strconv"
	"strings"
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

func sendInfoCommand(conn as.ClientIfc, command string) (map[string]string, error) {
	var err error
	var node *as.Node
	node, err = conn.Cluster().GetRandomNode()
	if err != nil {
		return nil, err
	}

	policy := as.NewInfoPolicy()
	requestRetVal, err := node.RequestInfo(policy, command)

	// iterate over requestRetVal, if the value has the substring ERROR in it then retrun the error into err
	for cmd, value := range requestRetVal {
		if strings.Contains(value, "ERROR") {
			err = errors.New("Error in request: " + cmd + " error was: " + value)
		}
	}

	return requestRetVal, err
}

// check is the capability is supported in the aerospike version we're connected to
func supportsCapability(conn as.ClientIfc, capability asCapabilities) (bool, error) {

	serverBuild, err := sendInfoCommand(conn, "build")
	if err != nil {
		return false, err
	}
	serverMajorVersion, err := strconv.Atoi(serverBuild["build"][0:1])
	if err != nil {
		panic(err)
	}

	switch capability {
	case SetLevelTTL:
		return serverMajorVersion >= int(SetLevelTTL), nil
	}

	return false, nil
}

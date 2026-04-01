// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"errors"
	"strings"

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

// sendInfoCommand sends an asinfo command to a random node in the cluster and returns the response.
// It checks the response for ERROR strings and returns an error if found.
func sendInfoCommand(conn as.ClientIfc, command string) (map[string]string, error) {
	node, err := conn.Cluster().GetRandomNode()
	if err != nil {
		return nil, err
	}

	policy := as.NewInfoPolicy()
	result, err := node.RequestInfo(policy, command)
	if err != nil {
		return nil, err
	}

	for cmd, value := range result {
		if strings.Contains(strings.ToUpper(value), "ERROR") {
			return nil, errors.New("error in asinfo request: " + cmd + " response: " + value)
		}
	}

	return result, nil
}

// getNamespaceConfig reads all namespace configuration parameters via get-config and returns them as a map.
func getNamespaceConfig(conn as.ClientIfc, namespace string) (map[string]string, error) {
	command := "get-config:context=namespace;id=" + namespace
	result, err := sendInfoCommand(conn, command)
	if err != nil {
		return nil, err
	}

	config := make(map[string]string)
	raw := result[command]
	pairs := strings.Split(raw, ";")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			config[kv[0]] = kv[1]
		}
	}
	return config, nil
}

// getSetConfig reads set-level info via "sets/<namespace>/<set>".
// Response format: "key=value:key=value:...;" (colon-separated, trailing semicolon).
// Returns parsed key-value pairs, or an empty map if the set doesn't exist.
func getSetConfig(conn as.ClientIfc, namespace, setName string) (map[string]string, error) {
	command := "sets/" + namespace + "/" + setName
	result, err := sendInfoCommand(conn, command)
	if err != nil {
		return nil, err
	}

	config := make(map[string]string)
	raw := strings.TrimRight(result[command], ";")
	if raw == "" {
		return config, nil
	}

	pairs := strings.Split(raw, ":")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			config[kv[0]] = kv[1]
		}
	}
	return config, nil
}

// getValidSetParamKeys returns the set of valid set-level param keys for a namespace.
// It tries reading the specific set first; if that's empty (set doesn't exist yet),
// it reads "sets/<namespace>" to find any existing set and uses its fields.
// This works because all sets in a namespace share the same available params for a given version.
func getValidSetParamKeys(conn as.ClientIfc, namespace, setName string) (map[string]bool, error) {
	// Try the target set first
	setInfo, err := getSetConfig(conn, namespace, setName)
	if err != nil {
		return nil, err
	}

	if len(setInfo) > 0 {
		keys := make(map[string]bool, len(setInfo))
		for k := range setInfo {
			keys[k] = true
		}
		return keys, nil
	}

	// Set doesn't exist yet — read "sets/<namespace>" for any existing set's fields
	command := "sets/" + namespace
	result, err := sendInfoCommand(conn, command)
	if err != nil {
		return nil, err
	}

	raw := strings.TrimRight(result[command], ";")
	if raw == "" {
		// No sets exist at all — can't validate, return nil to signal skip
		return nil, nil
	}

	// Response may contain multiple sets separated by ";", take the first one
	setEntries := strings.Split(raw, ";")
	keys := make(map[string]bool)
	pairs := strings.Split(setEntries[0], ":")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			keys[kv[0]] = true
		}
	}
	return keys, nil
}

// setNamespaceParam sets a single namespace-level configuration parameter via set-config.
func setNamespaceParam(conn as.ClientIfc, namespace, key, value string) (string, error) {
	command := "set-config:context=namespace;id=" + namespace + ";" + key + "=" + value
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// setNamespaceSetParam sets a single set-level configuration parameter within a namespace.
func setNamespaceSetParam(conn as.ClientIfc, namespace, setName, key, value string) (string, error) {
	command := "set-config:context=namespace;id=" + namespace + ";set=" + setName + ";" + key + "=" + value
	_, err := sendInfoCommand(conn, command)
	return command, err
}

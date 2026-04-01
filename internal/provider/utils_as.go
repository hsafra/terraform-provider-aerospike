// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"errors"
	"strings"
	"time"

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

// getServiceConfig reads all service configuration parameters via get-config and returns them as a map.
func getServiceConfig(conn as.ClientIfc) (map[string]string, error) {
	command := "get-config:context=service"
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

// setServiceParam sets a single service-level configuration parameter via set-config.
func setServiceParam(conn as.ClientIfc, key, value string) (string, error) {
	command := "set-config:context=service;" + key + "=" + value
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// getXDRDCConfig reads all DC-level XDR configuration parameters for a given datacenter.
func getXDRDCConfig(conn as.ClientIfc, dc string) (map[string]string, error) {
	command := "get-config:context=xdr;dc=" + dc
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

// getXDRDCNamespaceConfig reads namespace-level XDR configuration for a DC/namespace pair.
func getXDRDCNamespaceConfig(conn as.ClientIfc, dc, namespace string) (map[string]string, error) {
	command := "get-config:context=xdr;dc=" + dc + ";namespace=" + namespace
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

// dcExists checks whether a datacenter exists in the XDR configuration.
func dcExists(conn as.ClientIfc, dc string) bool {
	_, err := getXDRDCConfig(conn, dc)
	return err == nil
}

// createXDRDC creates a new datacenter in the XDR configuration.
func createXDRDC(conn as.ClientIfc, dc string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";action=create"
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// removeXDRDC removes a datacenter from the XDR configuration.
// All namespaces and nodes must be removed from the DC before calling this.
// Retries briefly because Aerospike may need a moment to fully detach removed
// namespaces and nodes before the DC can be deleted.
func removeXDRDC(conn as.ClientIfc, dc string) error {
	command := "set-config:context=xdr;dc=" + dc + ";action=delete"
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		_, err = sendInfoCommand(conn, command)
		if err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

// addXDRDCNode adds a node-address-port to a datacenter.
func addXDRDCNode(conn as.ClientIfc, dc, addrPort string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";node-address-port=" + addrPort + ";action=add"
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// removeXDRDCNode removes a node-address-port from a datacenter.
func removeXDRDCNode(conn as.ClientIfc, dc, addrPort string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";node-address-port=" + addrPort + ";action=remove"
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// addXDRDCNamespace adds a namespace to a datacenter, optionally with a rewind value.
// rewind can be "" (no rewind), "all", or a number of seconds.
func addXDRDCNamespace(conn as.ClientIfc, dc, namespace, rewind string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";action=add"
	if rewind != "" {
		command += ";rewind=" + rewind
	}
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// removeXDRDCNamespace removes a namespace from a datacenter.
func removeXDRDCNamespace(conn as.ClientIfc, dc, namespace string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";action=remove"
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// setXDRDCParam sets a DC-level XDR configuration parameter.
func setXDRDCParam(conn as.ClientIfc, dc, key, value string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";" + key + "=" + value
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// setXDRDCNamespaceParam sets a namespace-level XDR configuration parameter within a DC.
func setXDRDCNamespaceParam(conn as.ClientIfc, dc, namespace, key, value string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";" + key + "=" + value
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// addXDRDCNamespaceShipSet adds a ship-set to a namespace within a DC.
func addXDRDCNamespaceShipSet(conn as.ClientIfc, dc, namespace, setName string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";ship-set=" + setName
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// removeXDRDCNamespaceShipSet removes a ship-set from a namespace within a DC.
func removeXDRDCNamespaceShipSet(conn as.ClientIfc, dc, namespace, setName string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";ship-set=" + setName + ";action=remove"
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// addXDRDCNamespaceIgnoreSet adds an ignore-set to a namespace within a DC.
func addXDRDCNamespaceIgnoreSet(conn as.ClientIfc, dc, namespace, setName string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";ignore-set=" + setName
	_, err := sendInfoCommand(conn, command)
	return command, err
}

// removeXDRDCNamespaceIgnoreSet removes an ignore-set from a namespace within a DC.
func removeXDRDCNamespaceIgnoreSet(conn as.ClientIfc, dc, namespace, setName string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";ignore-set=" + setName + ";action=remove"
	_, err := sendInfoCommand(conn, command)
	return command, err
}

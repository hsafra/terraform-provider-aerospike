// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"errors"
	"fmt"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
)

type asCapabilities int

const (
	SetLevelTTL asCapabilities = 7
)

func namespaceExists(conn *as.Client, namespace string) bool {
	key, _ := as.NewKey(namespace, "dummy", "dummy")

	_, err := conn.Get(nil, key)

	return !err.Matches(astypes.INVALID_NAMESPACE)
}

// sendInfoCommand sends an asinfo command to a random node in the cluster and returns the response.
// Use this for read-only commands (get-config, etc.) where a single node's response is sufficient.
func sendInfoCommand(conn *as.Client, command string) (map[string]string, error) {
	node, err := conn.Cluster().GetRandomNode()
	if err != nil {
		return nil, err
	}

	return sendInfoToNode(node, command)
}

// sendInfoCommandAllNodes sends an asinfo command to ALL nodes in the cluster.
// Use this for write commands (set-config, etc.) because Aerospike set-config commands
// are per-node — they are NOT automatically distributed via SMD to other cluster members.
// This follows the same approach as asadm, which fans out config commands to every node.
func sendInfoCommandAllNodes(conn *as.Client, command string) (map[string]string, error) { //nolint:unparam // callers may use the result in the future
	nodes := conn.GetNodes()
	if len(nodes) == 0 {
		return nil, errors.New("no nodes available in cluster")
	}

	var lastResult map[string]string
	for _, node := range nodes {
		result, err := sendInfoToNode(node, command)
		if err != nil {
			return nil, fmt.Errorf("node %s: %w", node.GetName(), err)
		}
		lastResult = result
	}

	return lastResult, nil
}

// sendInfoToNode sends an asinfo command to a specific node and checks for errors.
func sendInfoToNode(node *as.Node, command string) (map[string]string, error) {
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
func getNamespaceConfig(conn *as.Client, namespace string) (map[string]string, error) {
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
func getSetConfig(conn *as.Client, namespace, setName string) (map[string]string, error) {
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
func getValidSetParamKeys(conn *as.Client, namespace, setName string) (map[string]bool, error) {
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
func setNamespaceParam(conn *as.Client, namespace, key, value string) (string, error) {
	command := "set-config:context=namespace;id=" + namespace + ";" + key + "=" + value
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// setNamespaceSetParam sets a single set-level configuration parameter within a namespace.
func setNamespaceSetParam(conn *as.Client, namespace, setName, key, value string) (string, error) {
	command := "set-config:context=namespace;id=" + namespace + ";set=" + setName + ";" + key + "=" + value
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// getServiceConfig reads all service configuration parameters via get-config and returns them as a map.
func getServiceConfig(conn *as.Client) (map[string]string, error) {
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
func setServiceParam(conn *as.Client, key, value string) (string, error) {
	command := "set-config:context=service;" + key + "=" + value
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// getXDRDCConfig reads all DC-level XDR configuration parameters for a given datacenter.
func getXDRDCConfig(conn *as.Client, dc string) (map[string]string, error) {
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
func getXDRDCNamespaceConfig(conn *as.Client, dc, namespace string) (map[string]string, error) {
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
func dcExists(conn *as.Client, dc string) bool {
	_, err := getXDRDCConfig(conn, dc)
	return err == nil
}

// createXDRDC creates a new datacenter in the XDR configuration.
// Structural XDR changes propagate via SMD, but we send to all nodes for consistency with asadm.
func createXDRDC(conn *as.Client, dc string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";action=create"
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// removeXDRDC removes a datacenter from the XDR configuration.
// All namespaces and nodes must be removed from the DC before calling this.
// Retries briefly because Aerospike may need a moment to fully detach removed
// namespaces and nodes before the DC can be deleted.
func removeXDRDC(conn *as.Client, dc string) error {
	command := "set-config:context=xdr;dc=" + dc + ";action=delete"
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		_, err = sendInfoCommandAllNodes(conn, command)
		if err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

// addXDRDCNode adds a node-address-port to a datacenter.
func addXDRDCNode(conn *as.Client, dc, addrPort string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";node-address-port=" + addrPort + ";action=add"
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// removeXDRDCNode removes a node-address-port from a datacenter.
func removeXDRDCNode(conn *as.Client, dc, addrPort string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";node-address-port=" + addrPort + ";action=remove"
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// addXDRDCNamespace adds a namespace to a datacenter, optionally with a rewind value.
// rewind can be "" (no rewind), "all", or a number of seconds.
func addXDRDCNamespace(conn *as.Client, dc, namespace, rewind string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";action=add"
	if rewind != "" {
		command += ";rewind=" + rewind
	}
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// removeXDRDCNamespace removes a namespace from a datacenter.
func removeXDRDCNamespace(conn *as.Client, dc, namespace string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";action=remove"
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// setXDRDCParam sets a DC-level XDR configuration parameter.
func setXDRDCParam(conn *as.Client, dc, key, value string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";" + key + "=" + value
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// setXDRDCNamespaceParam sets a namespace-level XDR configuration parameter within a DC.
func setXDRDCNamespaceParam(conn *as.Client, dc, namespace, key, value string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";" + key + "=" + value
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// addXDRDCNamespaceShipSet adds a single ship-set to a namespace within a DC.
func addXDRDCNamespaceShipSet(conn *as.Client, dc, namespace, setName string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";ship-set=" + setName
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

// addXDRDCNamespaceIgnoreSet adds a single ignore-set to a namespace within a DC.
func addXDRDCNamespaceIgnoreSet(conn *as.Client, dc, namespace, setName string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";ignore-set=" + setName
	_, err := sendInfoCommandAllNodes(conn, command)
	return command, err
}

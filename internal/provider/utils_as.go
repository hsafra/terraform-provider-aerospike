// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
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

// smdRaceError is the error Aerospike returns when a structural XDR command
// (create/delete DC, add/remove node/namespace) races with SMD propagation.
// For example, node 1 creates a DC and SMD propagates it to node 2 before our
// explicit command reaches node 2 → node 2 returns this error.
const smdRaceError = "ERROR:4:invalid state or set-config parameter"

// sendInfoCommandAllNodesSMD sends an asinfo command to ALL nodes, tolerating
// SMD propagation race errors on non-first nodes. Use this for structural XDR
// commands (create/delete DC, add/remove node/namespace) that are automatically
// distributed via SMD. The first node must succeed; subsequent nodes may fail
// with the expected SMD race error which is silently ignored.
func sendInfoCommandAllNodesSMD(conn *as.Client, command string) (map[string]string, error) { //nolint:unparam // callers may use the result in the future
	nodes := conn.GetNodes()
	if len(nodes) == 0 {
		return nil, errors.New("no nodes available in cluster")
	}

	// First node must succeed
	result, err := sendInfoToNode(nodes[0], command)
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", nodes[0].GetName(), err)
	}

	// Remaining nodes: tolerate the specific SMD race error, fail on anything else
	for _, node := range nodes[1:] {
		_, nodeErr := sendInfoToNode(node, command)
		if nodeErr != nil && !strings.Contains(nodeErr.Error(), smdRaceError) {
			return nil, fmt.Errorf("node %s: %w", node.GetName(), nodeErr)
		}
	}

	return result, nil
}

// infoRetryDelay is the pause before the single retry attempted by
// sendInfoToNode. Short on purpose: only meant to absorb a momentary
// network blip, not to mask a real failure.
const infoRetryDelay = 500 * time.Millisecond

// sendInfoToNode sends an asinfo command to a specific node. On any error
// it sleeps infoRetryDelay and retries once, so a transient network blip
// does not fail an entire plan or apply. After the second failure the
// error is returned and the caller fails fast.
func sendInfoToNode(node *as.Node, command string) (map[string]string, error) {
	result, err := sendInfoToNodeOnce(node, command)
	if err == nil {
		return result, nil
	}
	time.Sleep(infoRetryDelay)
	return sendInfoToNodeOnce(node, command)
}

// sendInfoToNodeOnce is a single attempt of an asinfo command — no retries.
func sendInfoToNodeOnce(node *as.Node, command string) (map[string]string, error) {
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

// nodeDivergence describes a single config key whose value differs across
// nodes. NodeValues maps nodeName -> value (only nodes that reported the
// key are listed).
type nodeDivergence struct {
	Key        string
	NodeValues map[string]string
}

// formatNodeValues renders a divergence's per-node values in stable order,
// suitable for embedding in a diagnostic message.
func (d nodeDivergence) formatNodeValues() string {
	names := make([]string, 0, len(d.NodeValues))
	for n := range d.NodeValues {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%s=%q", n, d.NodeValues[n]))
	}
	return strings.Join(parts, ", ")
}

// stringMapFromTypesMap extracts the string values of a types.Map into a
// plain map[string]string. Non-string elements and null/unknown values
// are skipped. Used to derive prior state for reduceNodeConfigs.
func stringMapFromTypesMap(m types.Map) map[string]string {
	out := make(map[string]string)
	if m.IsNull() || m.IsUnknown() {
		return out
	}
	for k, v := range m.Elements() {
		if s, ok := v.(types.String); ok && !s.IsNull() && !s.IsUnknown() {
			out[k] = s.ValueString()
		}
	}
	return out
}

// appendDivergenceWarnings emits one warning diagnostic per divergence whose
// key is present in `managed` (typically the priorState passed to
// reduceNodeConfigs, so only user-declared keys produce a warning). `where`
// is appended to the detail message to identify the context — e.g.
// "namespace \"foo\"" or "DC \"bar\""; pass "" when the context is implicit
// (e.g. the cluster-wide service config).
func appendDivergenceWarnings(diags *diag.Diagnostics, divergences []nodeDivergence, managed map[string]string, summary, where string) {
	for _, d := range divergences {
		if _, ok := managed[d.Key]; !ok {
			continue
		}
		var detail string
		if where == "" {
			detail = fmt.Sprintf("Parameter %q has different values across cluster nodes: %s. "+
				"Terraform will re-apply the configured value on the next apply to converge the cluster.",
				d.Key, d.formatNodeValues())
		} else {
			detail = fmt.Sprintf("Parameter %q on %s has different values across cluster nodes: %s. "+
				"Terraform will re-apply the configured value on the next apply to converge the cluster.",
				d.Key, where, d.formatNodeValues())
		}
		diags.AddWarning(summary, detail)
	}
}

// parseSemicolonKV parses a ";"-separated list of "key=value" pairs, the
// format Aerospike's get-config family returns. Empty or malformed pairs
// are silently skipped.
func parseSemicolonKV(raw string) map[string]string {
	out := make(map[string]string)
	for _, pair := range strings.Split(raw, ";") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
}

// fetchConfigAllNodes runs an asinfo command on every node and parses the
// response as ";"-separated key=value pairs. Returns nodeName -> key -> value.
// Fails fast if any node returns an error (after the per-node retry).
func fetchConfigAllNodes(conn *as.Client, command string) (map[string]map[string]string, error) {
	nodes := conn.GetNodes()
	if len(nodes) == 0 {
		return nil, errors.New("no nodes available in cluster")
	}

	out := make(map[string]map[string]string, len(nodes))
	for _, node := range nodes {
		result, err := sendInfoToNode(node, command)
		if err != nil {
			return nil, fmt.Errorf("node %s: %w", node.GetName(), err)
		}
		out[node.GetName()] = parseSemicolonKV(result[command])
	}
	return out, nil
}

// reduceNodeConfigs collapses per-node configs into a single config map and
// reports any keys where nodes disagree. When divergence is detected for a
// key, the chosen value prefers a node value that differs from priorState
// for that key — this writes a state that does not match the user's config,
// so tf-core plans a re-apply that will fan the desired value back out to
// all nodes. If no node value differs from prior state (or no prior state
// is known), the most common value wins.
func reduceNodeConfigs(nodeConfigs map[string]map[string]string, priorState map[string]string) (map[string]string, []nodeDivergence) {
	keys := make(map[string]struct{})
	for _, cfg := range nodeConfigs {
		for k := range cfg {
			keys[k] = struct{}{}
		}
	}

	reduced := make(map[string]string, len(keys))
	var divergences []nodeDivergence

	for key := range keys {
		valByNode := make(map[string]string, len(nodeConfigs))
		counts := make(map[string]int)
		for nodeName, cfg := range nodeConfigs {
			v, ok := cfg[key]
			if !ok {
				continue
			}
			valByNode[nodeName] = v
			counts[v]++
		}

		if len(counts) <= 1 {
			for _, v := range valByNode {
				reduced[key] = v
				break
			}
			continue
		}

		divergences = append(divergences, nodeDivergence{Key: key, NodeValues: valByNode})

		chosen, set := "", false
		if priorVal, hadPrior := priorState[key]; hadPrior {
			for _, v := range valByNode {
				if v != priorVal {
					chosen = v
					set = true
					break
				}
			}
		}
		if !set {
			bestCount := -1
			for v, c := range counts {
				if c > bestCount {
					bestCount = c
					chosen = v
				}
			}
		}
		reduced[key] = chosen
	}
	return reduced, divergences
}

// getNamespaceConfig reads all namespace configuration parameters via get-config and returns them as a map.
func getNamespaceConfig(conn *as.Client, namespace string) (map[string]string, error) {
	command := "get-config:context=namespace;id=" + namespace
	result, err := sendInfoCommand(conn, command)
	if err != nil {
		return nil, err
	}
	return parseSemicolonKV(result[command]), nil
}

// getNamespaceConfigAllNodes reads namespace config from every node and
// returns the reduced map plus any per-key divergences across nodes.
// priorState is the value previously persisted in Terraform state for the
// same keys — used to break ties in favor of a value that will force a
// drift+reapply (see reduceNodeConfigs).
func getNamespaceConfigAllNodes(conn *as.Client, namespace string, priorState map[string]string) (map[string]string, []nodeDivergence, error) {
	command := "get-config:context=namespace;id=" + namespace
	perNode, err := fetchConfigAllNodes(conn, command)
	if err != nil {
		return nil, nil, err
	}
	reduced, divergences := reduceNodeConfigs(perNode, priorState)
	return reduced, divergences, nil
}

// parseColonKV parses a single set entry, formatted as ":"-separated
// "key=value" pairs (Aerospike's "sets/<ns>/<set>" response format).
// Any trailing ";" is stripped before parsing.
func parseColonKV(raw string) map[string]string {
	out := make(map[string]string)
	raw = strings.TrimRight(raw, ";")
	if raw == "" {
		return out
	}
	for _, pair := range strings.Split(raw, ":") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
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
	return parseColonKV(result[command]), nil
}

// getSetConfigAllNodes reads set config from every node and returns the
// reduced map plus per-key divergences (see reduceNodeConfigs). priorState
// is the previously persisted Terraform state for the same set's keys.
func getSetConfigAllNodes(conn *as.Client, namespace, setName string, priorState map[string]string) (map[string]string, []nodeDivergence, error) {
	command := "sets/" + namespace + "/" + setName
	nodes := conn.GetNodes()
	if len(nodes) == 0 {
		return nil, nil, errors.New("no nodes available in cluster")
	}
	perNode := make(map[string]map[string]string, len(nodes))
	for _, node := range nodes {
		result, err := sendInfoToNode(node, command)
		if err != nil {
			return nil, nil, fmt.Errorf("node %s: %w", node.GetName(), err)
		}
		perNode[node.GetName()] = parseColonKV(result[command])
	}
	reduced, divergences := reduceNodeConfigs(perNode, priorState)
	return reduced, divergences, nil
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

// namespaceSubcontextPrefix is the prefix Aerospike's get-config uses for
// storage-engine parameters (e.g. "storage-engine.defrag-lwm-pct"). The
// set-config command requires this prefix to be stripped — only the bare
// parameter name is accepted (e.g. "defrag-lwm-pct").
// See https://aerospike.com/docs/database/tools/runtime-config/
const namespaceSubcontextPrefix = "storage-engine."

// stripNamespaceSubcontext removes the storage-engine subcontext prefix from a
// namespace parameter key so it can be used in a set-config command.
func stripNamespaceSubcontext(key string) string {
	if strings.HasPrefix(key, namespaceSubcontextPrefix) {
		return strings.TrimPrefix(key, namespaceSubcontextPrefix)
	}
	return key
}

// setNamespaceParam sets a single namespace-level configuration parameter via set-config.
// Parameters with the "storage-engine." prefix are automatically stripped because
// Aerospike's set-config does not accept the subcontext prefix.
func setNamespaceParam(conn *as.Client, namespace, key, value string) (string, error) {
	setKey := stripNamespaceSubcontext(key)
	command := "set-config:context=namespace;id=" + namespace + ";" + setKey + "=" + value
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
	return parseSemicolonKV(result[command]), nil
}

// getServiceConfigAllNodes reads service config from every node and returns
// the reduced map plus per-key divergences (see reduceNodeConfigs).
func getServiceConfigAllNodes(conn *as.Client, priorState map[string]string) (map[string]string, []nodeDivergence, error) {
	command := "get-config:context=service"
	perNode, err := fetchConfigAllNodes(conn, command)
	if err != nil {
		return nil, nil, err
	}
	reduced, divergences := reduceNodeConfigs(perNode, priorState)
	return reduced, divergences, nil
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
	return parseSemicolonKV(result[command]), nil
}

// getXDRDCConfigAllNodes reads XDR DC config from every node and returns
// the reduced map plus per-key divergences (see reduceNodeConfigs).
func getXDRDCConfigAllNodes(conn *as.Client, dc string, priorState map[string]string) (map[string]string, []nodeDivergence, error) {
	command := "get-config:context=xdr;dc=" + dc
	perNode, err := fetchConfigAllNodes(conn, command)
	if err != nil {
		return nil, nil, err
	}
	reduced, divergences := reduceNodeConfigs(perNode, priorState)
	return reduced, divergences, nil
}

// getXDRDCNamespaceConfig reads namespace-level XDR configuration for a DC/namespace pair.
func getXDRDCNamespaceConfig(conn *as.Client, dc, namespace string) (map[string]string, error) {
	command := "get-config:context=xdr;dc=" + dc + ";namespace=" + namespace
	result, err := sendInfoCommand(conn, command)
	if err != nil {
		return nil, err
	}
	return parseSemicolonKV(result[command]), nil
}

// getXDRDCNamespaceConfigAllNodes reads XDR DC namespace config from every
// node and returns the reduced map plus per-key divergences.
func getXDRDCNamespaceConfigAllNodes(conn *as.Client, dc, namespace string, priorState map[string]string) (map[string]string, []nodeDivergence, error) {
	command := "get-config:context=xdr;dc=" + dc + ";namespace=" + namespace
	perNode, err := fetchConfigAllNodes(conn, command)
	if err != nil {
		return nil, nil, err
	}
	reduced, divergences := reduceNodeConfigs(perNode, priorState)
	return reduced, divergences, nil
}

// dcExists checks whether a datacenter exists in the XDR configuration.
func dcExists(conn *as.Client, dc string) bool {
	_, err := getXDRDCConfig(conn, dc)
	return err == nil
}

// createXDRDC creates a new datacenter in the XDR configuration.
func createXDRDC(conn *as.Client, dc string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";action=create"
	_, err := sendInfoCommandAllNodesSMD(conn, command)
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
		_, err = sendInfoCommandAllNodesSMD(conn, command)
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
	_, err := sendInfoCommandAllNodesSMD(conn, command)
	return command, err
}

// removeXDRDCNode removes a node-address-port from a datacenter.
func removeXDRDCNode(conn *as.Client, dc, addrPort string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";node-address-port=" + addrPort + ";action=remove"
	_, err := sendInfoCommandAllNodesSMD(conn, command)
	return command, err
}

// addXDRDCNamespace adds a namespace to a datacenter, optionally with a rewind value.
// rewind can be "" (no rewind), "all", or a number of seconds.
func addXDRDCNamespace(conn *as.Client, dc, namespace, rewind string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";action=add"
	if rewind != "" {
		command += ";rewind=" + rewind
	}
	_, err := sendInfoCommandAllNodesSMD(conn, command)
	return command, err
}

// removeXDRDCNamespace removes a namespace from a datacenter.
func removeXDRDCNamespace(conn *as.Client, dc, namespace string) (string, error) {
	command := "set-config:context=xdr;dc=" + dc + ";namespace=" + namespace + ";action=remove"
	_, err := sendInfoCommandAllNodesSMD(conn, command)
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

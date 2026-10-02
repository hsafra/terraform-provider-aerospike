// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"maps"
	"slices"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// info_commands holds the asinfo commands that reproduce a resource's managed
// config, not a log of what the last apply sent. The builders below are the only
// place those commands are produced: apply sends what they return, Read rebuilds
// them from refreshed state, and ModifyPlan predicts them from the plan.
// Output is sorted so the same config always yields the same list.

const infoCommandsAttr = "info_commands"

// serviceInfoCommands returns the commands that set params in the service context.
func serviceInfoCommands(params map[string]string) []string {
	cmds := make([]string, 0, len(params))
	for _, key := range slices.Sorted(maps.Keys(params)) {
		cmds = append(cmds, serviceParamCommand(key, params[key]))
	}
	return cmds
}

// namespaceInfoCommands returns the commands that set params and setConfig on
// namespace: namespace-level params first, then sets, each sorted by name.
// skipEnableIndex lists sets whose enable-index is not sent (SMD-owned set indexes).
func namespaceInfoCommands(namespace string, params map[string]string, setConfig map[string]map[string]string, skipEnableIndex map[string]bool) []string {
	cmds := make([]string, 0, len(params))
	for _, key := range slices.Sorted(maps.Keys(params)) {
		cmds = append(cmds, namespaceParamCommand(namespace, key, params[key]))
	}
	for _, setName := range slices.Sorted(maps.Keys(setConfig)) {
		setParams := setConfig[setName]
		for _, key := range slices.Sorted(maps.Keys(setParams)) {
			if key == enableIndexParam && skipEnableIndex[setName] {
				continue
			}
			cmds = append(cmds, namespaceSetParamCommand(namespace, setName, key, setParams[key]))
		}
	}
	return cmds
}

// sindexInfoCommands returns the command that creates the set index.
func sindexInfoCommands(namespace, setName, name string) []string {
	return []string{sindexCreateSetCommand(namespace, setName, name)}
}

// sendInfoCommandsAllNodes sends cmds to every node in order, stopping at the first failure.
func sendInfoCommandsAllNodes(ctx context.Context, conn *as.Client, cmds []string) error {
	for _, cmd := range cmds {
		if _, err := sendInfoCommandAllNodes(conn, cmd); err != nil {
			return fmt.Errorf("command %q failed: %w", cmd, err)
		}
		tflog.Trace(ctx, "sent info command: "+cmd)
	}
	return nil
}

func infoCommandsList(ctx context.Context, cmds []string) (types.List, diag.Diagnostics) {
	if cmds == nil {
		cmds = []string{}
	}
	return types.ListValueFrom(ctx, types.StringType, cmds)
}

// planInfoCommands sets the planned info_commands when the framework has marked
// it unknown (the resource is being created or changed). A known planned value
// means nothing changed, so the prior state is kept. known=false leaves it
// unknown for values only decidable at apply time.
func planInfoCommands(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse, cmds []string, known bool) {
	var planned types.List
	resp.Diagnostics.Append(req.Plan.GetAttribute(ctx, path.Root(infoCommandsAttr), &planned)...)
	if resp.Diagnostics.HasError() || !planned.IsUnknown() || !known {
		return
	}
	list, diags := infoCommandsList(ctx, cmds)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.Plan.SetAttribute(ctx, path.Root(infoCommandsAttr), list)...)
}

// mapFullyKnown reports whether m and every value in it (recursing into nested
// maps) are known. Null counts as known.
func mapFullyKnown(m types.Map) bool {
	if m.IsUnknown() {
		return false
	}
	for _, v := range m.Elements() {
		if v.IsUnknown() {
			return false
		}
		if inner, ok := v.(types.Map); ok && !mapFullyKnown(inner) {
			return false
		}
	}
	return true
}

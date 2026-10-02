// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var _ resource.Resource = &AerospikeNamespaceConfig{}
var _ resource.ResourceWithImportState = &AerospikeNamespaceConfig{}
var _ resource.ResourceWithModifyPlan = &AerospikeNamespaceConfig{}

func NewAerospikeNamespaceConfig() resource.Resource {
	return &AerospikeNamespaceConfig{}
}

type AerospikeNamespaceConfig struct {
	asConn *asConnection
}

type AerospikeNamespaceConfigModel struct {
	Namespace    types.String `tfsdk:"namespace"`
	Params       types.Map    `tfsdk:"params"`
	SetConfig    types.Map    `tfsdk:"set_config"`
	InfoCommands types.List   `tfsdk:"info_commands"`
}

func (r *AerospikeNamespaceConfig) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namespace_config"
}

func (r *AerospikeNamespaceConfig) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages dynamic Aerospike namespace and set-level configuration parameters. " +
			"This resource only manages the parameters explicitly declared in the Terraform configuration — " +
			"all other server parameters are left untouched and will not cause drift. " +
			"Parameters are validated against the running server before being applied. " +
			"On destroy, parameters are NOT reset — they persist on the server until changed manually or the server is restarted. " +
			"On Database 8.1.2+, prefer aerospike_sindex over set_config enable-index for set-index lifecycle. " +
			"enable-index remains supported; removing it from the configuration does not disable the index. " +
			"Same-apply changes to enable-index and aerospike_sindex on the same set need an explicit depends_on.",

		Attributes: map[string]schema.Attribute{
			"namespace": schema.StringAttribute{
				Description: "Namespace name. Changing this forces recreation of the resource.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"params": schema.MapAttribute{
				Description: "Namespace-level configuration parameters as key-value string pairs. " +
					"Keys must be valid Aerospike namespace config parameter names for the connected server version.",
				Optional:    true,
				ElementType: types.StringType,
			},
			"set_config": schema.MapAttribute{
				Description: "Set-level configuration parameters. The outer map is keyed by set name, " +
					"and each value is a map of parameter key-value string pairs. " +
					"enable-index is deprecated on Database 8.1.2+ (use aerospike_sindex); " +
					"removing a key does not reset it on the server. " +
					"Same-apply enable-index and aerospike_sindex changes on one set need depends_on.",
				Optional:    true,
				ElementType: types.MapType{ElemType: types.StringType},
			},
			"info_commands": schema.ListAttribute{
				Description: "Output-only list of asinfo commands that reproduce the managed params and set_config: " +
					"namespace params sorted by key, then sets sorted by name with their params sorted by key. " +
					"Rebuilt from the server on every refresh, so it tracks the current config even when no apply runs. " +
					"enable-index is omitted for sets whose index is owned by aerospike_sindex. " +
					"Useful for persisting as commands to run when provisioning new servers.",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (r *AerospikeNamespaceConfig) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	asConn, ok := req.ProviderData.(*asConnection)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected asConnection, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.asConn = asConn
}

func (r *AerospikeNamespaceConfig) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data AerospikeNamespaceConfigModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	namespace := data.Namespace.ValueString()

	// Validate namespace exists
	if !namespaceExists(r.asConn.client, namespace) {
		resp.Diagnostics.AddError("Namespace not found",
			fmt.Sprintf("Namespace %q does not exist on the Aerospike server.", namespace))
		return
	}

	infoCommands, diags := r.applyNamespaceConfig(ctx, namespace, data.Params, data.SetConfig)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	cmdList, diags := infoCommandsList(ctx, infoCommands)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	data.InfoCommands = cmdList

	tflog.Trace(ctx, "applied namespace config to "+namespace)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeNamespaceConfig) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data AerospikeNamespaceConfigModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	namespace := data.Namespace.ValueString()

	// Check namespace still exists
	if !namespaceExists(r.asConn.client, namespace) {
		resp.State.RemoveResource(ctx)
		tflog.Trace(ctx, "namespace "+namespace+" no longer exists, removing from state")
		return
	}

	// Read current namespace config from every node so cross-node divergence
	// becomes a planned re-apply rather than a silently-masked drift.
	priorNsState := stringMapFromTypesMap(data.Params)
	serverConfig, nsDivergences, err := getNamespaceConfigAllNodes(r.asConn.client, namespace, priorNsState)
	if err != nil {
		resp.Diagnostics.AddError("Error reading namespace config",
			fmt.Sprintf("Could not read config for namespace %q: %s", namespace, err.Error()))
		return
	}

	// Update only user-managed namespace-level params from server.
	// On import, params will be null — we leave it null so only params
	// declared in the user's HCL config are tracked (avoids drift).
	if !data.Params.IsNull() {
		appendDivergenceWarnings(&resp.Diagnostics, nsDivergences, priorNsState,
			"Namespace parameter differs across cluster nodes",
			fmt.Sprintf("namespace %q", namespace))

		updatedParams := make(map[string]string)
		for key := range data.Params.Elements() {
			if serverVal, ok := serverConfig[key]; ok {
				updatedParams[key] = serverVal
			} else {
				resp.Diagnostics.AddWarning("Parameter not found on server",
					fmt.Sprintf("Parameter %q is in state but not found in server config for namespace %q. It may have been removed in this server version.", key, namespace))
			}
		}
		paramMap, diags := types.MapValueFrom(ctx, types.StringType, updatedParams)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		data.Params = paramMap
	}

	// Best-effort read of set-level params
	smdOwned := map[string]bool{}
	if !data.SetConfig.IsNull() {
		var smdDiags diag.Diagnostics
		smdOwned, _, smdDiags = r.smdOwnedSets(namespace, data.SetConfig)
		resp.Diagnostics.Append(smdDiags...)
		if resp.Diagnostics.HasError() {
			return
		}

		updatedSetConfig := make(map[string]map[string]string)
		for setName, priorSetState := range nestedStringMapFromTypesMap(data.SetConfig) {
			serverSetConfig, setDivergences, err := getSetConfigAllNodes(r.asConn.client, namespace, setName, priorSetState)
			if err != nil {
				resp.Diagnostics.AddError("Error reading set config",
					fmt.Sprintf("Failed to read set config for %s/%s: %s", namespace, setName, err.Error()))
				return
			}

			appendDivergenceWarnings(&resp.Diagnostics, setDivergences, priorSetState,
				"Set parameter differs across cluster nodes",
				fmt.Sprintf("set %q in namespace %q", setName, namespace))

			setParams := make(map[string]string)
			for key, val := range priorSetState {
				if key == enableIndexParam && smdOwned[setName] {
					setParams[key] = val
					continue
				}
				if serverVal, found := serverSetConfig[key]; found {
					setParams[key] = serverVal
				}
			}
			updatedSetConfig[setName] = setParams
		}

		setConfigMap, diags := types.MapValueFrom(ctx, types.MapType{ElemType: types.StringType}, updatedSetConfig)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		data.SetConfig = setConfigMap
	}

	// enable-index is not sent for SMD-owned sets, matching applyNamespaceConfig.
	cmdList, diags := infoCommandsList(ctx, namespaceInfoCommands(namespace,
		stringMapFromTypesMap(data.Params), nestedStringMapFromTypesMap(data.SetConfig), smdOwned))
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	data.InfoCommands = cmdList

	tflog.Trace(ctx, "read namespace config for "+namespace)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeNamespaceConfig) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state AerospikeNamespaceConfigModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	namespace := plan.Namespace.ValueString()

	// Apply all plan params — Aerospike set-config is idempotent
	infoCommands, diags := r.applyNamespaceConfig(ctx, namespace, plan.Params, plan.SetConfig)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Warn about removed namespace params
	if !state.Params.IsNull() && !plan.Params.IsNull() {
		for key := range state.Params.Elements() {
			if _, exists := plan.Params.Elements()[key]; !exists {
				resp.Diagnostics.AddWarning("Parameter removed from configuration",
					fmt.Sprintf("Parameter %q was removed from the Terraform configuration but cannot be unset on the server. "+
						"It retains its current value on namespace %q.", key, namespace))
			}
		}
	}

	warnRemovedSetConfig(&resp.Diagnostics, state.SetConfig, plan.SetConfig, namespace)

	cmdList, diags := infoCommandsList(ctx, infoCommands)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	data := plan
	data.InfoCommands = cmdList

	tflog.Trace(ctx, "updated namespace config for "+namespace)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

// ModifyPlan predicts info_commands from the planned params and set_config. On
// 8.1.2+, whether enable-index is sent depends on SMD ownership at apply time (an
// aerospike_sindex in the same apply can take over the set), so info_commands
// stays unknown there.
func (r *AerospikeNamespaceConfig) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.Plan.Raw.IsNull() {
		return
	}

	var plan AerospikeNamespaceConfigModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	known := !plan.Namespace.IsUnknown() && mapFullyKnown(plan.Params) && mapFullyKnown(plan.SetConfig)
	if known && setConfigHasParam(plan.SetConfig, enableIndexParam) {
		supports, err := serverSupportsSetSindex(r.asConn)
		known = err == nil && !supports
	}

	planInfoCommands(ctx, req, resp, namespaceInfoCommands(plan.Namespace.ValueString(),
		stringMapFromTypesMap(plan.Params), nestedStringMapFromTypesMap(plan.SetConfig), nil), known)
}

func (r *AerospikeNamespaceConfig) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data AerospikeNamespaceConfigModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.AddWarning("Namespace config not reset on destroy",
		fmt.Sprintf("Namespace configuration parameters for %q are not reset on destroy. "+
			"The values set by this resource will persist on the server until changed manually or the server is restarted.",
			data.Namespace.ValueString()))

	tflog.Trace(ctx, "destroyed namespace config resource for "+data.Namespace.ValueString()+" (params not reset)")
}

func (r *AerospikeNamespaceConfig) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("namespace"), req, resp)
}

// applyNamespaceConfig validates every namespace and set param, decides which
// sets skip enable-index, then sends the commands from namespaceInfoCommands.
// Nothing is sent if validation fails. It returns the commands sent.
func (r *AerospikeNamespaceConfig) applyNamespaceConfig(ctx context.Context, namespace string, paramsVal, setConfigVal types.Map) ([]string, diag.Diagnostics) {
	var diags diag.Diagnostics

	params := stringMapFromTypesMap(paramsVal)
	setConfig := nestedStringMapFromTypesMap(setConfigVal)

	diags.Append(r.validateNamespaceParams(namespace, params)...)
	diags.Append(r.validateSetParams(ctx, namespace, setConfig)...)
	if diags.HasError() {
		return nil, diags
	}

	skipEnableIndexSets, skipDiags := r.enableIndexSkips(namespace, setConfigVal, setConfig)
	diags.Append(skipDiags...)
	if diags.HasError() {
		return nil, diags
	}

	cmds := namespaceInfoCommands(namespace, params, setConfig, skipEnableIndexSets)
	if err := sendInfoCommandsAllNodes(ctx, r.asConn.client, cmds); err != nil {
		diags.AddError("Error applying namespace config",
			fmt.Sprintf("Failed to apply config to namespace %q: %s", namespace, err.Error()))
		return nil, diags
	}

	return cmds, diags
}

// validateNamespaceParams checks every key against the namespace's server config.
func (r *AerospikeNamespaceConfig) validateNamespaceParams(namespace string, params map[string]string) diag.Diagnostics {
	var diags diag.Diagnostics
	if len(params) == 0 {
		return diags
	}

	serverConfig, err := getNamespaceConfig(r.asConn.client, namespace)
	if err != nil {
		diags.AddError("Error reading namespace config",
			fmt.Sprintf("Could not read current config for namespace %q to validate parameters: %s", namespace, err.Error()))
		return diags
	}

	for _, key := range slices.Sorted(maps.Keys(params)) {
		if _, ok := serverConfig[key]; !ok {
			diags.AddError("Invalid namespace parameter",
				fmt.Sprintf("Parameter %q is not a valid namespace config parameter for namespace %q on this Aerospike server version.", key, namespace))
		}
	}
	return diags
}

// validateSetParams checks every set's keys against the params the server
// reports for sets in the namespace. Validation is skipped when the namespace
// has no sets yet.
func (r *AerospikeNamespaceConfig) validateSetParams(ctx context.Context, namespace string, setConfig map[string]map[string]string) diag.Diagnostics {
	var diags diag.Diagnostics

	for _, setName := range slices.Sorted(maps.Keys(setConfig)) {
		validKeys, err := getValidSetParamKeys(r.asConn.client, namespace, setName)
		if err != nil {
			diags.AddError("Error reading set config",
				fmt.Sprintf("Could not read set info for %q in namespace %q to validate parameters: %s", setName, namespace, err.Error()))
			return diags
		}
		if validKeys == nil {
			tflog.Trace(ctx, fmt.Sprintf("no existing sets in namespace %q to validate set param keys — skipping validation", namespace))
			continue
		}
		for _, key := range slices.Sorted(maps.Keys(setConfig[setName])) {
			if !validKeys[key] {
				diags.AddError("Invalid set parameter",
					fmt.Sprintf("Parameter %q is not a valid set-level config parameter for set %q in namespace %q on this Aerospike server version.", key, setName, namespace))
			}
		}
	}
	return diags
}

// enableIndexSkips returns the sets whose enable-index must not be sent (see
// skipEnableIndex), with the warnings or errors that decision produces.
func (r *AerospikeNamespaceConfig) enableIndexSkips(namespace string, setConfigVal types.Map, setConfig map[string]map[string]string) (map[string]bool, diag.Diagnostics) {
	skips := map[string]bool{}

	smdOwned, supports, diags := r.smdOwnedSets(namespace, setConfigVal)
	if diags.HasError() {
		return skips, diags
	}

	for _, setName := range slices.Sorted(maps.Keys(setConfig)) {
		value, ok := setConfig[setName][enableIndexParam]
		if !ok {
			continue
		}
		skip, skipDiags := skipEnableIndex(namespace, setName, value, supports, smdOwned[setName])
		diags.Append(skipDiags...)
		if skip {
			skips[setName] = true
		}
	}
	return skips, diags
}

func setConfigHasParam(setConfig types.Map, key string) bool {
	for _, inner := range nestedStringMapFromTypesMap(setConfig) {
		if _, has := inner[key]; has {
			return true
		}
	}
	return false
}

// smdOwnedSets returns sets with an SMD-owned set index when setConfig declares
// enable-index and the server is 8.1.2+. supports is true only on 8.1.2+.
func (r *AerospikeNamespaceConfig) smdOwnedSets(namespace string, setConfig types.Map) (owned map[string]bool, supports bool, diags diag.Diagnostics) {
	owned = map[string]bool{}
	if !setConfigHasParam(setConfig, enableIndexParam) {
		return owned, false, diags
	}

	ok, err := serverSupportsSetSindex(r.asConn)
	if err != nil {
		diags.AddError("Error reading Aerospike version",
			fmt.Sprintf("Could not determine server version: %s", err.Error()))
		return owned, false, diags
	}
	if !ok {
		return owned, false, diags
	}

	owned, err = smdSetIndexSets(r.asConn.client, namespace)
	if err != nil {
		diags.AddError("Error reading sindex list",
			fmt.Sprintf("Could not list sindexes for namespace %q to detect SMD-owned set indexes: %s", namespace, err.Error()))
		return owned, true, diags
	}
	return owned, true, diags
}

// skipEnableIndex reports whether set-config for enable-index should be skipped.
// On 8.1.2+, SMD-owned sets skip true (warn) and reject false (error). Config-owned
// sets still send the value, with a deprecation warning.
func skipEnableIndex(namespace, setName, value string, supports, smdOwned bool) (bool, diag.Diagnostics) {
	var diags diag.Diagnostics
	if !supports {
		return false, diags
	}
	if smdOwned {
		if strings.EqualFold(value, "false") {
			diags.AddError("Cannot disable SMD-owned set index via enable-index",
				fmt.Sprintf("Set %q in namespace %q already has an SMD-owned set index. "+
					"Destroy the aerospike_sindex resource instead of setting enable-index=false. "+
					"If this apply also destroys that aerospike_sindex, add depends_on so the sindex is destroyed first.",
					setName, namespace))
			return true, diags
		}
		diags.AddWarning("enable-index ignored (SMD-owned set index)",
			fmt.Sprintf("Set %q in namespace %q is SMD-owned; enable-index was not sent. "+
				"Remove it from set_config — the index is managed by aerospike_sindex.",
				setName, namespace))
		return true, diags
	}
	diags.AddWarning("enable-index is deprecated for set-index lifecycle",
		fmt.Sprintf("On Database 8.1.2+, prefer aerospike_sindex over set_config enable-index "+
			"for set %q in namespace %q. The value is still applied because this set is not SMD-owned.",
			setName, namespace))
	return false, diags
}

// warnRemovedSetConfig emits warnings when set_config sets or keys are dropped
// from HCL. Removed parameters are not reset on the server.
func warnRemovedSetConfig(diags *diag.Diagnostics, stateCfg, planCfg types.Map, namespace string) {
	if stateCfg.IsNull() || planCfg.IsUnknown() {
		return
	}

	planSets := nestedStringMapFromTypesMap(planCfg)
	for setName, stateKeys := range nestedStringMapFromTypesMap(stateCfg) {
		planKeys, setStillPresent := planSets[setName]
		if !setStillPresent {
			diags.AddWarning("Set configuration removed from configuration",
				fmt.Sprintf("Set %q was removed from set_config but its parameters cannot be unset on the server. "+
					"They retain their current values in namespace %q.", setName, namespace))
			continue
		}
		for key := range stateKeys {
			if _, exists := planKeys[key]; !exists {
				diags.AddWarning("Set parameter removed from configuration",
					fmt.Sprintf("Parameter %q was removed from set %q in the Terraform configuration but cannot be unset on the server. "+
						"It retains its current value in namespace %q.", key, setName, namespace))
			}
		}
	}
}

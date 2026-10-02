// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var _ resource.Resource = &AerospikeServiceConfig{}
var _ resource.ResourceWithImportState = &AerospikeServiceConfig{}
var _ resource.ResourceWithModifyPlan = &AerospikeServiceConfig{}

func NewAerospikeServiceConfig() resource.Resource {
	return &AerospikeServiceConfig{}
}

type AerospikeServiceConfig struct {
	asConn *asConnection
}

type AerospikeServiceConfigModel struct {
	Params       types.Map  `tfsdk:"params"`
	InfoCommands types.List `tfsdk:"info_commands"`
}

func (r *AerospikeServiceConfig) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_service_config"
}

func (r *AerospikeServiceConfig) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages dynamic Aerospike service-level configuration parameters. " +
			"Only one instance of this resource is allowed per provider — the Aerospike service context is a cluster-wide singleton. " +
			"This resource only manages the parameters explicitly declared in the Terraform configuration — " +
			"all other server parameters are left untouched and will not cause drift. " +
			"Parameters are validated against the running server before being applied. " +
			"On destroy, parameters are NOT reset — they persist on the server until changed manually or the server is restarted.",

		Attributes: map[string]schema.Attribute{
			"params": schema.MapAttribute{
				Description: "Service-level configuration parameters as key-value string pairs. " +
					"Keys must be valid dynamic Aerospike service config parameter names for the connected server version.",
				Optional:    true,
				ElementType: types.StringType,
			},
			"info_commands": schema.ListAttribute{
				Description: "Output-only list of asinfo commands that reproduce the managed params, sorted by key. " +
					"Rebuilt from the server on every refresh, so it tracks the current config even when no apply runs. " +
					"Useful for persisting as commands to run when provisioning new servers.",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (r *AerospikeServiceConfig) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *AerospikeServiceConfig) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	// Enforce singleton: only one aerospike_service_config resource per provider
	if !atomic.CompareAndSwapInt32(&r.asConn.serviceConfigClaimed, 0, 1) {
		resp.Diagnostics.AddError("Duplicate service config resource",
			"Only one aerospike_service_config resource is allowed per provider. "+
				"The Aerospike service context is a cluster-wide singleton.")
		return
	}

	var data AerospikeServiceConfigModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		atomic.StoreInt32(&r.asConn.serviceConfigClaimed, 0)
		return
	}

	infoCommands, diags := r.applyServiceParams(ctx, stringMapFromTypesMap(data.Params))
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		atomic.StoreInt32(&r.asConn.serviceConfigClaimed, 0)
		return
	}

	cmdList, diags := infoCommandsList(ctx, infoCommands)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		atomic.StoreInt32(&r.asConn.serviceConfigClaimed, 0)
		return
	}
	data.InfoCommands = cmdList

	tflog.Trace(ctx, "applied service config")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeServiceConfig) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data AerospikeServiceConfigModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Read current service config from every node so cross-node divergence
	// becomes a planned re-apply rather than a silently-masked drift.
	priorState := stringMapFromTypesMap(data.Params)
	serverConfig, divergences, err := getServiceConfigAllNodes(r.asConn.client, priorState)
	if err != nil {
		resp.Diagnostics.AddError("Error reading service config",
			fmt.Sprintf("Could not read service config: %s", err.Error()))
		return
	}

	// Update only user-managed params from server.
	// On import, params will be null — we leave it null so only params
	// declared in the user's HCL config are tracked (avoids drift).
	if !data.Params.IsNull() {
		appendDivergenceWarnings(&resp.Diagnostics, divergences, priorState,
			"Service parameter differs across cluster nodes", "")

		updatedParams := make(map[string]string)
		for key := range data.Params.Elements() {
			if serverVal, ok := serverConfig[key]; ok {
				updatedParams[key] = serverVal
			} else {
				resp.Diagnostics.AddWarning("Parameter not found on server",
					fmt.Sprintf("Parameter %q is in state but not found in server service config. It may have been removed in this server version.", key))
			}
		}
		paramMap, diags := types.MapValueFrom(ctx, types.StringType, updatedParams)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		data.Params = paramMap
	}

	cmdList, diags := infoCommandsList(ctx, serviceInfoCommands(stringMapFromTypesMap(data.Params)))
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	data.InfoCommands = cmdList

	tflog.Trace(ctx, "read service config")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeServiceConfig) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state AerospikeServiceConfigModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Apply all plan params — Aerospike set-config is idempotent
	infoCommands, diags := r.applyServiceParams(ctx, stringMapFromTypesMap(plan.Params))
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Warn about removed service params
	if !state.Params.IsNull() && !plan.Params.IsNull() {
		for key := range state.Params.Elements() {
			if _, exists := plan.Params.Elements()[key]; !exists {
				resp.Diagnostics.AddWarning("Parameter removed from configuration",
					fmt.Sprintf("Parameter %q was removed from the Terraform configuration but cannot be unset on the server. "+
						"It retains its current value in the service context.", key))
			}
		}
	}

	cmdList, diags := infoCommandsList(ctx, infoCommands)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	data := plan
	data.InfoCommands = cmdList

	tflog.Trace(ctx, "updated service config")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

// ModifyPlan predicts info_commands from the planned params.
func (r *AerospikeServiceConfig) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.Plan.Raw.IsNull() {
		return
	}

	var params types.Map
	resp.Diagnostics.Append(req.Plan.GetAttribute(ctx, path.Root("params"), &params)...)
	if resp.Diagnostics.HasError() {
		return
	}

	planInfoCommands(ctx, req, resp, serviceInfoCommands(stringMapFromTypesMap(params)), mapFullyKnown(params))
}

func (r *AerospikeServiceConfig) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data AerospikeServiceConfigModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Release singleton claim
	atomic.StoreInt32(&r.asConn.serviceConfigClaimed, 0)

	resp.Diagnostics.AddWarning("Service config not reset on destroy",
		"Service configuration parameters are not reset on destroy. "+
			"The values set by this resource will persist on the server until changed manually or the server is restarted.")

	tflog.Trace(ctx, "destroyed service config resource (params not reset)")
}

func (r *AerospikeServiceConfig) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	// Service config is a singleton — accept any import ID (conventionally "service")
	// and set empty state so the next plan+apply tracks user-declared params.
	resp.Diagnostics.Append(resp.State.Set(ctx, &AerospikeServiceConfigModel{
		Params:       types.MapNull(types.StringType),
		InfoCommands: types.ListNull(types.StringType),
	})...)
}

// applyServiceParams validates every key against the server, then sends the
// commands from serviceInfoCommands. Nothing is sent if any key is invalid.
// It returns the commands sent.
func (r *AerospikeServiceConfig) applyServiceParams(ctx context.Context, params map[string]string) ([]string, diag.Diagnostics) {
	var diags diag.Diagnostics

	if len(params) == 0 {
		return []string{}, diags
	}

	serverConfig, err := getServiceConfig(r.asConn.client)
	if err != nil {
		diags.AddError("Error reading service config",
			fmt.Sprintf("Could not read current service config to validate parameters: %s", err.Error()))
		return nil, diags
	}

	for _, key := range slices.Sorted(maps.Keys(params)) {
		if _, ok := serverConfig[key]; !ok {
			diags.AddError("Invalid service parameter",
				fmt.Sprintf("Parameter %q is not a valid service config parameter on this Aerospike server version.", key))
		}
	}
	if diags.HasError() {
		return nil, diags
	}

	cmds := serviceInfoCommands(params)
	if err := sendInfoCommandsAllNodes(ctx, r.asConn.client, cmds); err != nil {
		diags.AddError("Error setting service parameter", err.Error())
		return nil, diags
	}

	return cmds, diags
}

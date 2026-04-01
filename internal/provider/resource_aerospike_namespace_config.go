// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"

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
			"On destroy, parameters are NOT reset — they persist on the server until changed manually or the server is restarted.",

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
					"and each value is a map of parameter key-value string pairs.",
				Optional:    true,
				ElementType: types.MapType{ElemType: types.StringType},
			},
			"info_commands": schema.ListAttribute{
				Description: "Output-only list of all asinfo commands executed during the last create or update. " +
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
	if !namespaceExists(*r.asConn.client, namespace) {
		resp.Diagnostics.AddError("Namespace not found",
			fmt.Sprintf("Namespace %q does not exist on the Aerospike server.", namespace))
		return
	}

	var infoCommands []string

	// Apply namespace-level params
	if !data.Params.IsNull() && !data.Params.IsUnknown() {
		diags := r.applyNamespaceParams(ctx, namespace, data.Params, &infoCommands)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	// Apply set-level params
	if !data.SetConfig.IsNull() && !data.SetConfig.IsUnknown() {
		diags := r.applySetParams(ctx, namespace, data.SetConfig, &infoCommands)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	// Build info_commands list
	cmdList, diags := types.ListValueFrom(ctx, types.StringType, infoCommands)
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
	if !namespaceExists(*r.asConn.client, namespace) {
		resp.State.RemoveResource(ctx)
		tflog.Trace(ctx, "namespace "+namespace+" no longer exists, removing from state")
		return
	}

	// Read current namespace config from server
	serverConfig, err := getNamespaceConfig(*r.asConn.client, namespace)
	if err != nil {
		resp.Diagnostics.AddError("Error reading namespace config",
			fmt.Sprintf("Could not read config for namespace %q: %s", namespace, err.Error()))
		return
	}

	// Update only user-managed namespace-level params from server.
	// On import, params will be null — we leave it null so only params
	// declared in the user's HCL config are tracked (avoids drift).
	if !data.Params.IsNull() {
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
	if !data.SetConfig.IsNull() {
		updatedSetConfig := make(map[string]map[string]string)
		for setName, innerVal := range data.SetConfig.Elements() {
			innerMap, ok := innerVal.(types.Map)
			if !ok {
				continue
			}

			serverSetConfig, err := getSetConfig(*r.asConn.client, namespace, setName)
			if err != nil {
				// Best-effort: keep state as-is for this set
				tflog.Trace(ctx, fmt.Sprintf("could not read set config for %s/%s: %s", namespace, setName, err.Error()))
				stateParams := make(map[string]string)
				for k, v := range innerMap.Elements() {
					if sv, ok := v.(types.String); ok {
						stateParams[k] = sv.ValueString()
					}
				}
				updatedSetConfig[setName] = stateParams
				continue
			}

			setParams := make(map[string]string)
			for key, val := range innerMap.Elements() {
				if sv, ok := val.(types.String); ok {
					if serverVal, found := serverSetConfig[key]; found {
						setParams[key] = serverVal
					} else {
						// Keep state value if not readable from server
						setParams[key] = sv.ValueString()
					}
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

	// info_commands preserved from state; initialize to empty list if null (e.g., after import)
	if data.InfoCommands.IsNull() {
		emptyList, diags := types.ListValueFrom(ctx, types.StringType, []string{})
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		data.InfoCommands = emptyList
	}

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
	var infoCommands []string

	// Apply namespace-level params (set all plan params — Aerospike set-config is idempotent)
	if !plan.Params.IsNull() && !plan.Params.IsUnknown() {
		diags := r.applyNamespaceParams(ctx, namespace, plan.Params, &infoCommands)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
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

	// Apply set-level params
	if !plan.SetConfig.IsNull() && !plan.SetConfig.IsUnknown() {
		diags := r.applySetParams(ctx, namespace, plan.SetConfig, &infoCommands)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	// Build info_commands list
	cmdList, diags := types.ListValueFrom(ctx, types.StringType, infoCommands)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	data := plan
	data.InfoCommands = cmdList

	tflog.Trace(ctx, "updated namespace config for "+namespace)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
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

// applyNamespaceParams validates and applies namespace-level parameters.
func (r *AerospikeNamespaceConfig) applyNamespaceParams(ctx context.Context, namespace string, params types.Map, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	// Read current config to validate param keys
	serverConfig, err := getNamespaceConfig(*r.asConn.client, namespace)
	if err != nil {
		diags.AddError("Error reading namespace config",
			fmt.Sprintf("Could not read current config for namespace %q to validate parameters: %s", namespace, err.Error()))
		return diags
	}

	// Validate all keys exist, then apply
	for key, val := range params.Elements() {
		if _, ok := serverConfig[key]; !ok {
			diags.AddError("Invalid namespace parameter",
				fmt.Sprintf("Parameter %q is not a valid namespace config parameter for namespace %q on this Aerospike server version.", key, namespace))
		}

		strVal, ok := val.(types.String)
		if !ok {
			diags.AddError("Invalid parameter value",
				fmt.Sprintf("Parameter %q has a non-string value.", key))
		}

		if diags.HasError() {
			continue
		}

		command, err := setNamespaceParam(*r.asConn.client, namespace, key, strVal.ValueString())
		if err != nil {
			diags.AddError("Error setting namespace parameter",
				fmt.Sprintf("Failed to set parameter %q=%q on namespace %q: %s", key, strVal.ValueString(), namespace, err.Error()))
			return diags
		}

		tflog.Trace(ctx, "set namespace param: "+command)
		*infoCommands = append(*infoCommands, command)
	}

	return diags
}

// applySetParams validates and applies set-level parameters.
// It reads available set params from the server to validate keys before setting them.
func (r *AerospikeNamespaceConfig) applySetParams(ctx context.Context, namespace string, setConfig types.Map, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	for setName, innerVal := range setConfig.Elements() {
		innerMap, ok := innerVal.(types.Map)
		if !ok {
			diags.AddError("Invalid set_config value",
				fmt.Sprintf("Expected a map of parameters for set %q, got unexpected type.", setName))
			continue
		}

		// Validate set param keys against server
		validKeys, err := getValidSetParamKeys(*r.asConn.client, namespace, setName)
		if err != nil {
			diags.AddError("Error reading set config",
				fmt.Sprintf("Could not read set info for %q in namespace %q to validate parameters: %s", setName, namespace, err.Error()))
			return diags
		}

		if validKeys != nil {
			for key := range innerMap.Elements() {
				if !validKeys[key] {
					diags.AddError("Invalid set parameter",
						fmt.Sprintf("Parameter %q is not a valid set-level config parameter for set %q in namespace %q on this Aerospike server version.", key, setName, namespace))
				}
			}
			if diags.HasError() {
				return diags
			}
		} else {
			tflog.Trace(ctx, fmt.Sprintf("no existing sets in namespace %q to validate set param keys — skipping validation", namespace))
		}

		for key, val := range innerMap.Elements() {
			strVal, ok := val.(types.String)
			if !ok {
				diags.AddError("Invalid parameter value",
					fmt.Sprintf("Parameter %q for set %q has a non-string value.", key, setName))
				continue
			}

			command, err := setNamespaceSetParam(*r.asConn.client, namespace, setName, key, strVal.ValueString())
			if err != nil {
				diags.AddError("Error setting set parameter",
					fmt.Sprintf("Failed to set parameter %q=%q on set %q in namespace %q: %s",
						key, strVal.ValueString(), setName, namespace, err.Error()))
				return diags
			}

			tflog.Trace(ctx, "set set-level param: "+command)
			*infoCommands = append(*infoCommands, command)
		}
	}

	return diags
}

// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var _ resource.Resource = &AerospikeXDRDCConfig{}
var _ resource.ResourceWithImportState = &AerospikeXDRDCConfig{}

func NewAerospikeXDRDCConfig() resource.Resource {
	return &AerospikeXDRDCConfig{}
}

type AerospikeXDRDCConfig struct {
	asConn *asConnection
}

type AerospikeXDRDCConfigModel struct {
	DC               types.String        `tfsdk:"dc"`
	NodeAddressPorts types.List          `tfsdk:"node_address_ports"`
	Params           types.Map           `tfsdk:"params"`
	Namespaces       []XDRNamespaceModel `tfsdk:"namespace"`
	InfoCommands     types.List          `tfsdk:"info_commands"`
}

type XDRNamespaceModel struct {
	Name      types.String        `tfsdk:"name"`
	Rewind    types.String        `tfsdk:"rewind"`
	Params    types.Map           `tfsdk:"params"`
	SetPolicy []XDRSetPolicyModel `tfsdk:"set_policy"`
}

type XDRSetPolicyModel struct {
	ShipOnlySpecifiedSets types.Bool `tfsdk:"ship_only_specified_sets"`
	ShipSets              types.Set  `tfsdk:"ship_sets"`
	IgnoreSets            types.Set  `tfsdk:"ignore_sets"`
}

func (r *AerospikeXDRDCConfig) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_xdr_dc_config"
}

func (r *AerospikeXDRDCConfig) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages an Aerospike XDR datacenter configuration. " +
			"Creates the datacenter, adds nodes and namespaces, and applies dynamic configuration parameters. " +
			"This resource only manages the parameters explicitly declared in the Terraform configuration — " +
			"all other server parameters are left untouched and will not cause drift. " +
			"On destroy, the datacenter is removed from the XDR configuration.",

		Attributes: map[string]schema.Attribute{
			"dc": schema.StringAttribute{
				Description: "Datacenter name. Changing this forces recreation of the resource.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"node_address_ports": schema.ListAttribute{
				Description: "List of node address:port strings to add to this datacenter (e.g. [\"10.0.0.2:3000\"]).",
				Optional:    true,
				ElementType: types.StringType,
			},
			"params": schema.MapAttribute{
				Description: "DC-level XDR configuration parameters as key-value string pairs.",
				Optional:    true,
				ElementType: types.StringType,
			},
			"info_commands": schema.ListAttribute{
				Description: "Output-only list of all asinfo commands executed during the last create or update.",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
		Blocks: map[string]schema.Block{
			"namespace": schema.ListNestedBlock{
				Description: "Namespace configurations within this datacenter.",
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Description: "Namespace name.",
							Required:    true,
						},
						"rewind": schema.StringAttribute{
							Description: "Rewind value used when initially adding the namespace. " +
								"Can be \"all\" or a number of seconds. Only used on create, not on subsequent updates.",
							Optional: true,
						},
						"params": schema.MapAttribute{
							Description: "Namespace-level XDR configuration parameters as key-value string pairs. " +
								"Do not include ship-only-specified-sets, ship-set, or ignore-set here — use the set_policy block instead.",
							Optional:    true,
							ElementType: types.StringType,
						},
					},
					Blocks: map[string]schema.Block{
						"set_policy": schema.ListNestedBlock{
							Description: "Set shipping policy for this namespace. " +
								"Controls which sets are shipped or ignored. At most one set_policy block is allowed.",
							Validators: []validator.List{
								listvalidator.SizeAtMost(1),
							},
							NestedObject: schema.NestedBlockObject{
								Attributes: map[string]schema.Attribute{
									"ship_only_specified_sets": schema.BoolAttribute{
										Description: "When true, only sets listed in ship_sets are shipped. " +
											"When false (default), all sets are shipped except those in ignore_sets.",
										Required: true,
									},
									"ship_sets": schema.SetAttribute{
										Description: "Sets to ship. Only valid when ship_only_specified_sets is true.",
										Optional:    true,
										ElementType: types.StringType,
									},
									"ignore_sets": schema.SetAttribute{
										Description: "Sets to ignore. Only valid when ship_only_specified_sets is false.",
										Optional:    true,
										ElementType: types.StringType,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func (r *AerospikeXDRDCConfig) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *AerospikeXDRDCConfig) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data AerospikeXDRDCConfigModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dc := data.DC.ValueString()
	var infoCommands []string

	// Validate all config before applying
	resp.Diagnostics.Append(r.validateConfig(ctx, data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Create the DC
	cmd, err := createXDRDC(*r.asConn.client, dc)
	if err != nil {
		resp.Diagnostics.AddError("Error creating XDR datacenter",
			fmt.Sprintf("Failed to create datacenter %q: %s", dc, err.Error()))
		return
	}
	infoCommands = append(infoCommands, cmd)

	// Add nodes
	resp.Diagnostics.Append(r.applyNodes(ctx, dc, data.NodeAddressPorts, &infoCommands)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Apply DC-level params
	if !data.Params.IsNull() && !data.Params.IsUnknown() {
		resp.Diagnostics.Append(r.applyDCParams(ctx, dc, data.Params, &infoCommands)...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	// Add and configure namespaces
	for _, ns := range data.Namespaces {
		resp.Diagnostics.Append(r.addAndConfigureNamespace(ctx, dc, ns, &infoCommands)...)
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

	tflog.Trace(ctx, "created XDR DC config for "+dc)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeXDRDCConfig) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data AerospikeXDRDCConfigModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dc := data.DC.ValueString()

	// Check DC still exists
	if !dcExists(*r.asConn.client, dc) {
		resp.State.RemoveResource(ctx)
		tflog.Trace(ctx, "XDR DC "+dc+" no longer exists, removing from state")
		return
	}

	// Read DC-level params
	if !data.Params.IsNull() {
		serverConfig, err := getXDRDCConfig(*r.asConn.client, dc)
		if err != nil {
			resp.Diagnostics.AddError("Error reading XDR DC config",
				fmt.Sprintf("Could not read config for DC %q: %s", dc, err.Error()))
			return
		}

		updatedParams := make(map[string]string)
		for key := range data.Params.Elements() {
			if serverVal, ok := serverConfig[key]; ok {
				updatedParams[key] = serverVal
			} else {
				resp.Diagnostics.AddWarning("Parameter not found on server",
					fmt.Sprintf("Parameter %q is in state but not found in DC %q config.", key, dc))
			}
		}
		paramMap, diags := types.MapValueFrom(ctx, types.StringType, updatedParams)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		data.Params = paramMap
	}

	// Read namespace-level params
	for i, ns := range data.Namespaces {
		nsName := ns.Name.ValueString()

		if !ns.Params.IsNull() {
			serverNsConfig, err := getXDRDCNamespaceConfig(*r.asConn.client, dc, nsName)
			if err != nil {
				tflog.Trace(ctx, fmt.Sprintf("could not read XDR DC namespace config for %s/%s: %s", dc, nsName, err.Error()))
				continue
			}

			updatedParams := make(map[string]string)
			for key := range ns.Params.Elements() {
				if serverVal, ok := serverNsConfig[key]; ok {
					updatedParams[key] = serverVal
				}
			}
			paramMap, diags := types.MapValueFrom(ctx, types.StringType, updatedParams)
			resp.Diagnostics.Append(diags...)
			if resp.Diagnostics.HasError() {
				return
			}
			data.Namespaces[i].Params = paramMap
		}

		// Read set policy state from server config
		if len(ns.SetPolicy) > 0 {
			serverNsConfig, err := getXDRDCNamespaceConfig(*r.asConn.client, dc, nsName)
			if err != nil {
				tflog.Trace(ctx, fmt.Sprintf("could not read XDR DC namespace config for set_policy %s/%s: %s", dc, nsName, err.Error()))
				continue
			}

			policy := ns.SetPolicy[0]

			// Read ship-only-specified-sets from server
			if sosVal, ok := serverNsConfig["ship-only-specified-sets"]; ok {
				policy.ShipOnlySpecifiedSets = types.BoolValue(sosVal == "true")
			}

			// ship-set and ignore-set are list-type params; the server returns them
			// as comma-separated or colon-separated values. We keep the user's
			// declared state since these are action-based (add/remove) rather than
			// directly readable as a flat config key.
			data.Namespaces[i].SetPolicy[0] = policy
		}
	}

	// Preserve info_commands from state; initialize if null
	if data.InfoCommands.IsNull() {
		emptyList, diags := types.ListValueFrom(ctx, types.StringType, []string{})
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		data.InfoCommands = emptyList
	}

	tflog.Trace(ctx, "read XDR DC config for "+dc)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeXDRDCConfig) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state AerospikeXDRDCConfigModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dc := plan.DC.ValueString()
	var infoCommands []string

	// Validate all config before applying
	resp.Diagnostics.Append(r.validateConfig(ctx, plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Diff nodes: remove old, add new
	resp.Diagnostics.Append(r.diffNodes(ctx, dc, state.NodeAddressPorts, plan.NodeAddressPorts, &infoCommands)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Apply DC-level params
	if !plan.Params.IsNull() && !plan.Params.IsUnknown() {
		resp.Diagnostics.Append(r.applyDCParams(ctx, dc, plan.Params, &infoCommands)...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	// Warn about removed DC params
	if !state.Params.IsNull() && !plan.Params.IsNull() {
		for key := range state.Params.Elements() {
			if _, exists := plan.Params.Elements()[key]; !exists {
				resp.Diagnostics.AddWarning("Parameter removed from configuration",
					fmt.Sprintf("DC parameter %q was removed from the Terraform configuration but cannot be unset on the server.", key))
			}
		}
	}

	// Diff namespaces
	stateNsMap := make(map[string]XDRNamespaceModel)
	for _, ns := range state.Namespaces {
		stateNsMap[ns.Name.ValueString()] = ns
	}
	planNsMap := make(map[string]XDRNamespaceModel)
	for _, ns := range plan.Namespaces {
		planNsMap[ns.Name.ValueString()] = ns
	}

	// Remove namespaces no longer in plan
	for nsName := range stateNsMap {
		if _, exists := planNsMap[nsName]; !exists {
			cmd, err := removeXDRDCNamespace(*r.asConn.client, dc, nsName)
			if err != nil {
				resp.Diagnostics.AddError("Error removing XDR DC namespace",
					fmt.Sprintf("Failed to remove namespace %q from DC %q: %s", nsName, dc, err.Error()))
				return
			}
			infoCommands = append(infoCommands, cmd)
		}
	}

	// Add new namespaces and update existing ones
	for _, ns := range plan.Namespaces {
		nsName := ns.Name.ValueString()
		if _, exists := stateNsMap[nsName]; !exists {
			// New namespace — add it
			resp.Diagnostics.Append(r.addAndConfigureNamespace(ctx, dc, ns, &infoCommands)...)
		} else {
			// Existing namespace — update params and set policy
			resp.Diagnostics.Append(r.updateNamespaceConfig(ctx, dc, ns, stateNsMap[nsName], &infoCommands)...)
		}
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

	tflog.Trace(ctx, "updated XDR DC config for "+dc)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeXDRDCConfig) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data AerospikeXDRDCConfigModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dc := data.DC.ValueString()

	// Remove namespaces first
	for _, ns := range data.Namespaces {
		_, err := removeXDRDCNamespace(*r.asConn.client, dc, ns.Name.ValueString())
		if err != nil {
			tflog.Trace(ctx, fmt.Sprintf("could not remove namespace %q from DC %q during destroy: %s",
				ns.Name.ValueString(), dc, err.Error()))
		}
	}

	// Remove nodes (required before DC can be deleted)
	if !data.NodeAddressPorts.IsNull() {
		for _, elem := range data.NodeAddressPorts.Elements() {
			if addr, ok := elem.(types.String); ok {
				_, err := removeXDRDCNode(*r.asConn.client, dc, addr.ValueString())
				if err != nil {
					tflog.Trace(ctx, fmt.Sprintf("could not remove node %q from DC %q during destroy: %s",
						addr.ValueString(), dc, err.Error()))
				}
			}
		}
	}

	// Delete the DC (retries internally — Aerospike may need a moment
	// after namespace/node removal before the DC can be deleted)
	err := removeXDRDC(*r.asConn.client, dc)
	if err != nil {
		resp.Diagnostics.AddError("Error removing XDR datacenter",
			fmt.Sprintf("Failed to remove datacenter %q: %s", dc, err.Error()))
		return
	}

	tflog.Trace(ctx, "destroyed XDR DC config for "+dc)
}

func (r *AerospikeXDRDCConfig) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	dc := req.ID

	if !dcExists(*r.asConn.client, dc) {
		resp.Diagnostics.AddError("DC not found",
			fmt.Sprintf("Datacenter %q does not exist on the Aerospike server.", dc))
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &AerospikeXDRDCConfigModel{
		DC:               types.StringValue(dc),
		NodeAddressPorts: types.ListNull(types.StringType),
		Params:           types.MapNull(types.StringType),
		Namespaces:       nil,
		InfoCommands:     types.ListNull(types.StringType),
	})...)
}

// validateConfig performs all validation before any changes are applied.
func (r *AerospikeXDRDCConfig) validateConfig(_ context.Context, data AerospikeXDRDCConfigModel) diag.Diagnostics {
	var diags diag.Diagnostics

	// Validate DC params don't contain reserved set-policy keys
	if !data.Params.IsNull() && !data.Params.IsUnknown() {
		for key := range data.Params.Elements() {
			if xdrReservedSetPolicyKeys[key] {
				diags.AddError("Reserved parameter in DC params",
					fmt.Sprintf("Parameter %q must be managed via the set_policy block within a namespace, not in DC-level params.", key))
			}
		}
	}

	for _, ns := range data.Namespaces {
		// Validate namespace params don't contain reserved set-policy keys
		if !ns.Params.IsNull() && !ns.Params.IsUnknown() {
			for key := range ns.Params.Elements() {
				if xdrReservedSetPolicyKeys[key] {
					diags.AddError("Reserved parameter in namespace params",
						fmt.Sprintf("Parameter %q must be managed via the set_policy block, not in namespace params for %q.", key, ns.Name.ValueString()))
				}
			}
		}

		// Validate set_policy block
		if len(ns.SetPolicy) > 0 {
			policy := ns.SetPolicy[0]
			shipOnly := policy.ShipOnlySpecifiedSets.ValueBool()

			hasShipSets := !policy.ShipSets.IsNull() && !policy.ShipSets.IsUnknown() && len(policy.ShipSets.Elements()) > 0
			hasIgnoreSets := !policy.IgnoreSets.IsNull() && !policy.IgnoreSets.IsUnknown() && len(policy.IgnoreSets.Elements()) > 0

			if hasShipSets && hasIgnoreSets {
				diags.AddError("Parameter incompatibility: set_policy",
					fmt.Sprintf("Namespace %q: ship_sets and ignore_sets cannot both be specified. "+
						"Use ship_sets with ship_only_specified_sets=true, or ignore_sets with ship_only_specified_sets=false.",
						ns.Name.ValueString()))
			}

			if hasShipSets && !shipOnly {
				diags.AddError("Parameter incompatibility: set_policy",
					fmt.Sprintf("Namespace %q: ship_sets requires ship_only_specified_sets=true.",
						ns.Name.ValueString()))
			}

			if hasIgnoreSets && shipOnly {
				diags.AddError("Parameter incompatibility: set_policy",
					fmt.Sprintf("Namespace %q: ignore_sets requires ship_only_specified_sets=false.",
						ns.Name.ValueString()))
			}
		}
	}

	return diags
}

// applyNodes adds all nodes from the list to the DC.
func (r *AerospikeXDRDCConfig) applyNodes(_ context.Context, dc string, nodes types.List, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	if nodes.IsNull() || nodes.IsUnknown() {
		return diags
	}

	for _, elem := range nodes.Elements() {
		addr, ok := elem.(types.String)
		if !ok {
			diags.AddError("Invalid node_address_ports value", "Expected string elements.")
			return diags
		}

		cmd, err := addXDRDCNode(*r.asConn.client, dc, addr.ValueString())
		if err != nil {
			diags.AddError("Error adding XDR DC node",
				fmt.Sprintf("Failed to add node %q to DC %q: %s", addr.ValueString(), dc, err.Error()))
			return diags
		}
		*infoCommands = append(*infoCommands, cmd)
	}

	return diags
}

// diffNodes computes the diff between old and new node lists and applies changes.
func (r *AerospikeXDRDCConfig) diffNodes(_ context.Context, dc string, oldNodes, newNodes types.List, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	oldSet := make(map[string]bool)
	newSet := make(map[string]bool)

	if !oldNodes.IsNull() {
		for _, elem := range oldNodes.Elements() {
			if s, ok := elem.(types.String); ok {
				oldSet[s.ValueString()] = true
			}
		}
	}

	if !newNodes.IsNull() {
		for _, elem := range newNodes.Elements() {
			if s, ok := elem.(types.String); ok {
				newSet[s.ValueString()] = true
			}
		}
	}

	// Remove nodes no longer in plan
	for addr := range oldSet {
		if !newSet[addr] {
			cmd, err := removeXDRDCNode(*r.asConn.client, dc, addr)
			if err != nil {
				diags.AddError("Error removing XDR DC node",
					fmt.Sprintf("Failed to remove node %q from DC %q: %s", addr, dc, err.Error()))
				return diags
			}
			*infoCommands = append(*infoCommands, cmd)
		}
	}

	// Add new nodes
	for addr := range newSet {
		if !oldSet[addr] {
			cmd, err := addXDRDCNode(*r.asConn.client, dc, addr)
			if err != nil {
				diags.AddError("Error adding XDR DC node",
					fmt.Sprintf("Failed to add node %q to DC %q: %s", addr, dc, err.Error()))
				return diags
			}
			*infoCommands = append(*infoCommands, cmd)
		}
	}

	return diags
}

// applyDCParams applies DC-level parameters.
func (r *AerospikeXDRDCConfig) applyDCParams(ctx context.Context, dc string, params types.Map, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	for key, val := range params.Elements() {
		strVal, ok := val.(types.String)
		if !ok {
			diags.AddError("Invalid parameter value",
				fmt.Sprintf("DC parameter %q has a non-string value.", key))
			continue
		}

		cmd, err := setXDRDCParam(*r.asConn.client, dc, key, strVal.ValueString())
		if err != nil {
			diags.AddError("Error setting XDR DC parameter",
				fmt.Sprintf("Failed to set DC parameter %q=%q on DC %q: %s", key, strVal.ValueString(), dc, err.Error()))
			return diags
		}

		tflog.Trace(ctx, "set XDR DC param: "+cmd)
		*infoCommands = append(*infoCommands, cmd)
	}

	return diags
}

// addAndConfigureNamespace adds a namespace to the DC and applies all its configuration.
func (r *AerospikeXDRDCConfig) addAndConfigureNamespace(ctx context.Context, dc string, ns XDRNamespaceModel, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	nsName := ns.Name.ValueString()
	rewind := ""
	if !ns.Rewind.IsNull() && !ns.Rewind.IsUnknown() {
		rewind = ns.Rewind.ValueString()
	}

	// Add namespace to DC first
	cmd, err := addXDRDCNamespace(*r.asConn.client, dc, nsName, rewind)
	if err != nil {
		diags.AddError("Error adding XDR DC namespace",
			fmt.Sprintf("Failed to add namespace %q to DC %q: %s", nsName, dc, err.Error()))
		return diags
	}
	*infoCommands = append(*infoCommands, cmd)

	// Apply namespace-level params
	if !ns.Params.IsNull() && !ns.Params.IsUnknown() {
		diags.Append(r.applyNamespaceParams(ctx, dc, nsName, ns.Params, infoCommands)...)
		if diags.HasError() {
			return diags
		}
	}

	// Apply set_policy after namespace is added
	if len(ns.SetPolicy) > 0 {
		diags.Append(r.applySetPolicy(ctx, dc, nsName, ns.SetPolicy[0], infoCommands)...)
		if diags.HasError() {
			return diags
		}
	}

	return diags
}

// updateNamespaceConfig applies param and set_policy changes to an existing namespace.
func (r *AerospikeXDRDCConfig) updateNamespaceConfig(ctx context.Context, dc string, plan, state XDRNamespaceModel, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	nsName := plan.Name.ValueString()

	// Apply namespace-level params (idempotent)
	if !plan.Params.IsNull() && !plan.Params.IsUnknown() {
		diags.Append(r.applyNamespaceParams(ctx, dc, nsName, plan.Params, infoCommands)...)
		if diags.HasError() {
			return diags
		}
	}

	// Diff set_policy
	diags.Append(r.diffSetPolicy(ctx, dc, nsName, state.SetPolicy, plan.SetPolicy, infoCommands)...)

	return diags
}

// applyNamespaceParams applies namespace-level XDR params.
func (r *AerospikeXDRDCConfig) applyNamespaceParams(ctx context.Context, dc, namespace string, params types.Map, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	for key, val := range params.Elements() {
		strVal, ok := val.(types.String)
		if !ok {
			diags.AddError("Invalid parameter value",
				fmt.Sprintf("Namespace parameter %q has a non-string value.", key))
			continue
		}

		cmd, err := setXDRDCNamespaceParam(*r.asConn.client, dc, namespace, key, strVal.ValueString())
		if err != nil {
			diags.AddError("Error setting XDR DC namespace parameter",
				fmt.Sprintf("Failed to set parameter %q=%q on namespace %q in DC %q: %s",
					key, strVal.ValueString(), namespace, dc, err.Error()))
			return diags
		}

		tflog.Trace(ctx, "set XDR DC namespace param: "+cmd)
		*infoCommands = append(*infoCommands, cmd)
	}

	return diags
}

// applySetPolicy applies a set_policy block from scratch.
func (r *AerospikeXDRDCConfig) applySetPolicy(_ context.Context, dc, namespace string, policy XDRSetPolicyModel, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	// Set ship-only-specified-sets
	sosVal := "false"
	if policy.ShipOnlySpecifiedSets.ValueBool() {
		sosVal = "true"
	}
	cmd, err := setXDRDCNamespaceParam(*r.asConn.client, dc, namespace, "ship-only-specified-sets", sosVal)
	if err != nil {
		diags.AddError("Error setting ship-only-specified-sets",
			fmt.Sprintf("Failed on namespace %q in DC %q: %s", namespace, dc, err.Error()))
		return diags
	}
	*infoCommands = append(*infoCommands, cmd)

	// Add ship-sets
	if !policy.ShipSets.IsNull() && !policy.ShipSets.IsUnknown() {
		for _, elem := range policy.ShipSets.Elements() {
			setName, ok := elem.(types.String)
			if !ok {
				continue
			}
			cmd, err := addXDRDCNamespaceShipSet(*r.asConn.client, dc, namespace, setName.ValueString())
			if err != nil {
				diags.AddError("Error adding ship-set",
					fmt.Sprintf("Failed to add ship-set %q on namespace %q in DC %q: %s",
						setName.ValueString(), namespace, dc, err.Error()))
				return diags
			}
			*infoCommands = append(*infoCommands, cmd)
		}
	}

	// Add ignore-sets
	if !policy.IgnoreSets.IsNull() && !policy.IgnoreSets.IsUnknown() {
		for _, elem := range policy.IgnoreSets.Elements() {
			setName, ok := elem.(types.String)
			if !ok {
				continue
			}
			cmd, err := addXDRDCNamespaceIgnoreSet(*r.asConn.client, dc, namespace, setName.ValueString())
			if err != nil {
				diags.AddError("Error adding ignore-set",
					fmt.Sprintf("Failed to add ignore-set %q on namespace %q in DC %q: %s",
						setName.ValueString(), namespace, dc, err.Error()))
				return diags
			}
			*infoCommands = append(*infoCommands, cmd)
		}
	}

	return diags
}

// diffSetPolicy computes diffs between old and new set policies and applies changes.
func (r *AerospikeXDRDCConfig) diffSetPolicy(_ context.Context, dc, namespace string, oldPolicy, newPolicy []XDRSetPolicyModel, infoCommands *[]string) diag.Diagnostics {
	var diags diag.Diagnostics

	// If new policy exists, apply it
	if len(newPolicy) > 0 {
		np := newPolicy[0]

		// Always set ship-only-specified-sets
		sosVal := "false"
		if np.ShipOnlySpecifiedSets.ValueBool() {
			sosVal = "true"
		}
		cmd, err := setXDRDCNamespaceParam(*r.asConn.client, dc, namespace, "ship-only-specified-sets", sosVal)
		if err != nil {
			diags.AddError("Error setting ship-only-specified-sets",
				fmt.Sprintf("Failed on namespace %q in DC %q: %s", namespace, dc, err.Error()))
			return diags
		}
		*infoCommands = append(*infoCommands, cmd)

		// Compute ship-set diff
		oldShipSets := extractStringSet(oldPolicy, func(p XDRSetPolicyModel) types.Set { return p.ShipSets })
		newShipSets := extractStringSetValues(np.ShipSets)

		for s := range oldShipSets {
			if !newShipSets[s] {
				cmd, err := removeXDRDCNamespaceShipSet(*r.asConn.client, dc, namespace, s)
				if err != nil {
					diags.AddError("Error removing ship-set",
						fmt.Sprintf("Failed to remove ship-set %q: %s", s, err.Error()))
					return diags
				}
				*infoCommands = append(*infoCommands, cmd)
			}
		}
		for s := range newShipSets {
			if !oldShipSets[s] {
				cmd, err := addXDRDCNamespaceShipSet(*r.asConn.client, dc, namespace, s)
				if err != nil {
					diags.AddError("Error adding ship-set",
						fmt.Sprintf("Failed to add ship-set %q: %s", s, err.Error()))
					return diags
				}
				*infoCommands = append(*infoCommands, cmd)
			}
		}

		// Compute ignore-set diff
		oldIgnoreSets := extractStringSet(oldPolicy, func(p XDRSetPolicyModel) types.Set { return p.IgnoreSets })
		newIgnoreSets := extractStringSetValues(np.IgnoreSets)

		for s := range oldIgnoreSets {
			if !newIgnoreSets[s] {
				cmd, err := removeXDRDCNamespaceIgnoreSet(*r.asConn.client, dc, namespace, s)
				if err != nil {
					diags.AddError("Error removing ignore-set",
						fmt.Sprintf("Failed to remove ignore-set %q: %s", s, err.Error()))
					return diags
				}
				*infoCommands = append(*infoCommands, cmd)
			}
		}
		for s := range newIgnoreSets {
			if !oldIgnoreSets[s] {
				cmd, err := addXDRDCNamespaceIgnoreSet(*r.asConn.client, dc, namespace, s)
				if err != nil {
					diags.AddError("Error adding ignore-set",
						fmt.Sprintf("Failed to add ignore-set %q: %s", s, err.Error()))
					return diags
				}
				*infoCommands = append(*infoCommands, cmd)
			}
		}
	} else if len(oldPolicy) > 0 {
		// Set policy removed — reset ship-only-specified-sets to default
		cmd, err := setXDRDCNamespaceParam(*r.asConn.client, dc, namespace, "ship-only-specified-sets", "false")
		if err != nil {
			diags.AddError("Error resetting ship-only-specified-sets",
				fmt.Sprintf("Failed on namespace %q in DC %q: %s", namespace, dc, err.Error()))
			return diags
		}
		*infoCommands = append(*infoCommands, cmd)

		// Remove all old ship-sets and ignore-sets
		oldShipSets := extractStringSet(oldPolicy, func(p XDRSetPolicyModel) types.Set { return p.ShipSets })
		for s := range oldShipSets {
			cmd, _ := removeXDRDCNamespaceShipSet(*r.asConn.client, dc, namespace, s)
			*infoCommands = append(*infoCommands, cmd)
		}
		oldIgnoreSets := extractStringSet(oldPolicy, func(p XDRSetPolicyModel) types.Set { return p.IgnoreSets })
		for s := range oldIgnoreSets {
			cmd, _ := removeXDRDCNamespaceIgnoreSet(*r.asConn.client, dc, namespace, s)
			*infoCommands = append(*infoCommands, cmd)
		}
	}

	return diags
}

// extractStringSet extracts a set of strings from the first element of a policy slice.
func extractStringSet(policies []XDRSetPolicyModel, getter func(XDRSetPolicyModel) types.Set) map[string]bool {
	result := make(map[string]bool)
	if len(policies) == 0 {
		return result
	}
	s := getter(policies[0])
	if s.IsNull() || s.IsUnknown() {
		return result
	}
	for _, elem := range s.Elements() {
		if str, ok := elem.(types.String); ok {
			result[str.ValueString()] = true
		}
	}
	return result
}

// extractStringSetValues extracts a set of strings from a types.Set.
func extractStringSetValues(s types.Set) map[string]bool {
	result := make(map[string]bool)
	if s.IsNull() || s.IsUnknown() {
		return result
	}
	for _, elem := range s.Elements() {
		if str, ok := elem.(types.String); ok {
			result[str.ValueString()] = true
		}
	}
	return result
}

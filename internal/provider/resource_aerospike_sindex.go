// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var _ resource.Resource = &AerospikeSindex{}
var _ resource.ResourceWithImportState = &AerospikeSindex{}
var _ resource.ResourceWithModifyPlan = &AerospikeSindex{}

func NewAerospikeSindex() resource.Resource {
	return &AerospikeSindex{}
}

type AerospikeSindex struct {
	asConn *asConnection
}

type AerospikeSindexModel struct {
	ID           types.String `tfsdk:"id"`
	Namespace    types.String `tfsdk:"namespace"`
	Set          types.String `tfsdk:"set"`
	Name         types.String `tfsdk:"name"`
	IndexType    types.String `tfsdk:"index_type"`
	InfoCommands types.List   `tfsdk:"info_commands"`
}

func (r *AerospikeSindex) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_sindex"
}

func (r *AerospikeSindex) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages Aerospike set indexes on Database 8.1.2+ via sindex-create / sindex-delete. " +
			"Requires the sindex-admin privilege (or data-admin / sys-admin). " +
			"Creating a set index on a config-owned (enable-index) index converts ownership in place with no rebuild. " +
			"Destroy uses sindex-delete only. Changing name renames the index in place. " +
			"Same-apply changes to aerospike_sindex and set_config enable-index on one set need an explicit depends_on.",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Description: "Import identifier: namespace/set/name.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"namespace": schema.StringAttribute{
				Description: "Namespace name. At most 31 characters; must not contain ':', ';', '/', '=', or '|'. " +
					"Changing this forces recreation of the resource.",
				Required: true,
				Validators: []validator.String{
					sindexIdentValidator{maxLen: asNamespaceMaxLen, what: "namespace"},
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"set": schema.StringAttribute{
				Description: "Set name. Required for set indexes. At most 63 characters; must not contain ':', ';', '/', '=', or '|'. " +
					"Changing this forces recreation of the resource.",
				Required: true,
				Validators: []validator.String{
					sindexIdentValidator{maxLen: asSetNameMaxLen, what: "set"},
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Index name (sindex indexname). Required. At most 63 characters; must not contain ':', ';', '/', '=', or '|'. " +
					"Changing the name renames the existing set index on the server (no rebuild).",
				Required: true,
				Validators: []validator.String{
					sindexIdentValidator{maxLen: asSindexNameMaxLen, what: "index name"},
				},
			},
			"index_type": schema.StringAttribute{
				Description: "Index type. Currently only \"set\" is supported. Changing this forces recreation of the resource.",
				Required:    true,
				Validators: []validator.String{
					stringvalidator.OneOf("set"),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"info_commands": schema.ListAttribute{
				Description: "Output-only list of asinfo commands executed during the last create or update. " +
					"Useful for persisting asinfo commands to run when provisioning new servers.",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (r *AerospikeSindex) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// ModifyPlan sets id from namespace/set/name so a name change updates the
// planned id. UseStateForUnknown would otherwise keep the old id and fail apply.
func (r *AerospikeSindex) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.Plan.Raw.IsNull() {
		return
	}

	var plan AerospikeSindexModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if plan.Namespace.IsUnknown() || plan.Set.IsUnknown() || plan.Name.IsUnknown() {
		return
	}

	plan.ID = types.StringValue(sindexID(plan.Namespace.ValueString(), plan.Set.ValueString(), plan.Name.ValueString()))
	resp.Diagnostics.Append(resp.Plan.Set(ctx, &plan)...)
}

func (r *AerospikeSindex) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data AerospikeSindexModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if !r.requireSetSindexSupport(&resp.Diagnostics) {
		return
	}

	namespace := data.Namespace.ValueString()
	setName := data.Set.ValueString()
	name := data.Name.ValueString()

	if !namespaceExists(r.asConn.client, namespace) {
		resp.Diagnostics.AddError("Namespace not found",
			fmt.Sprintf("Namespace %q does not exist on the Aerospike server.", namespace))
		return
	}

	existing, err := getSetIndex(r.asConn.client, namespace, setName)
	if err != nil {
		resp.Diagnostics.AddError("Error reading set index",
			fmt.Sprintf("Could not list sindexes for namespace %q: %s", namespace, err.Error()))
		return
	}
	if err := refuseSindexCreateRename(existing, namespace, setName, name); err != nil {
		resp.Diagnostics.AddError("Set index already exists", err.Error())
		return
	}

	command, err := createSetSindex(r.asConn.client, namespace, setName, name)
	if err != nil {
		resp.Diagnostics.AddError("Error creating set index",
			fmt.Sprintf("Failed to create set index %q on %s/%s: %s", name, namespace, setName, err.Error()))
		return
	}

	cmdList, diags := types.ListValueFrom(ctx, types.StringType, []string{command})
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	data.InfoCommands = cmdList
	data.ID = types.StringValue(sindexID(namespace, setName, name))

	tflog.Trace(ctx, "created set index "+command)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeSindex) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data AerospikeSindexModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	namespace := data.Namespace.ValueString()
	name := data.Name.ValueString()

	if !namespaceExists(r.asConn.client, namespace) {
		resp.State.RemoveResource(ctx)
		tflog.Trace(ctx, "namespace "+namespace+" no longer exists, removing set index from state")
		return
	}

	entry, err := getSindexByName(r.asConn.client, namespace, name)
	if err != nil {
		resp.Diagnostics.AddError("Error reading sindex",
			fmt.Sprintf("Could not list sindexes in namespace %q: %s", namespace, err.Error()))
		return
	}

	if entry == nil || !isSetIndex(*entry) {
		resp.State.RemoveResource(ctx)
		tflog.Trace(ctx, "set index "+name+" no longer exists in sindex-list, removing from state")
		return
	}

	data.Set = types.StringValue(entry.Set)
	data.Name = types.StringValue(entry.Name)
	data.IndexType = types.StringValue("set")
	data.ID = types.StringValue(sindexID(entry.Namespace, entry.Set, entry.Name))

	if data.InfoCommands.IsNull() {
		emptyList, diags := types.ListValueFrom(ctx, types.StringType, []string{})
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		data.InfoCommands = emptyList
	}

	tflog.Trace(ctx, "read set index "+data.ID.ValueString())

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeSindex) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan AerospikeSindexModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if !r.requireSetSindexSupport(&resp.Diagnostics) {
		return
	}

	namespace := plan.Namespace.ValueString()
	setName := plan.Set.ValueString()
	name := plan.Name.ValueString()

	command, err := createSetSindex(r.asConn.client, namespace, setName, name)
	if err != nil {
		resp.Diagnostics.AddError("Error updating set index",
			fmt.Sprintf("Failed to apply set index %q on %s/%s (rename uses sindex-create): %s",
				name, namespace, setName, err.Error()))
		return
	}

	cmdList, diags := types.ListValueFrom(ctx, types.StringType, []string{command})
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	plan.InfoCommands = cmdList
	plan.ID = types.StringValue(sindexID(namespace, setName, name))

	tflog.Trace(ctx, "updated set index "+command)

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *AerospikeSindex) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data AerospikeSindexModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	namespace := data.Namespace.ValueString()
	setName := data.Set.ValueString()
	name := data.Name.ValueString()

	command, err := deleteSetSindex(r.asConn.client, namespace, setName, name)
	if err != nil {
		resp.Diagnostics.AddError("Error deleting set index",
			fmt.Sprintf("Failed to delete set index %q on %s/%s via sindex-delete: %s",
				name, namespace, setName, err.Error()))
		return
	}

	tflog.Trace(ctx, "deleted set index "+command)
}

func (r *AerospikeSindex) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	namespace, setName, name, err := parseSindexImportID(req.ID)
	if err != nil {
		resp.Diagnostics.AddError("Invalid import ID", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), req.ID)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("namespace"), namespace)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("set"), setName)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("name"), name)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("index_type"), "set")...)
}

func (r *AerospikeSindex) requireSetSindexSupport(diags *diag.Diagnostics) bool {
	ok, err := serverSupportsSetSindex(r.asConn)
	if err != nil {
		diags.AddError("Error reading Aerospike version",
			fmt.Sprintf("Could not determine server version: %s", err.Error()))
		return false
	}
	if !ok {
		diags.AddError("Set indexes via sindex-create require Aerospike 8.1.2+",
			"aerospike_sindex with index_type=\"set\" needs Database 8.1.2 or later. "+
				"On older servers, manage set indexes with aerospike_namespace_config set_config enable-index.")
		return false
	}
	return true
}

type sindexIdentValidator struct {
	maxLen int
	what   string
}

func (v sindexIdentValidator) Description(_ context.Context) string {
	return fmt.Sprintf("%s at most %d characters, must not contain ':', ';', '/', '=', or '|'", v.what, v.maxLen)
}

func (v sindexIdentValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (v sindexIdentValidator) ValidateString(_ context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	if err := checkSindexIdent(req.ConfigValue.ValueString(), v.maxLen, v.what); err != nil {
		resp.Diagnostics.AddAttributeError(req.Path, "Invalid "+v.what, err.Error())
	}
}

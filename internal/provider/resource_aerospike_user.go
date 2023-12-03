// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	astypes "github.com/aerospike/aerospike-client-go/v6/types"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"reflect"
	"sort"
	"strings"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.Resource = &AerospikeUser{}
var _ resource.ResourceWithImportState = &AerospikeUser{}

func NewAerospikeUser() resource.Resource {
	return &AerospikeUser{}
}

// AerospikeUser defines the resource implementation.
type AerospikeUser struct {
	asConn *asConnection
}

// AerospikeUserModel describes the resource data model.
type AerospikeUserModel struct {
	User_name types.String   `tfsdk:"user_name"`
	Password  types.String   `tfsdk:"password"`
	Roles     []types.String `tfsdk:"roles"`
}

func (r *AerospikeUser) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_user"
}

func (r *AerospikeUser) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		// This description is used by the documentation generator and the language server.
		MarkdownDescription: "Aerospike user",

		Attributes: map[string]schema.Attribute{
			"user_name": schema.StringAttribute{
				Description: "User name",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"password": schema.StringAttribute{
				Description: "Password",
				Required:    true,
				Sensitive:   true,
			},
			"roles": schema.ListAttribute{
				Description: "Roles that should be granted to the user",
				Optional:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (r *AerospikeUser) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
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

func (r *AerospikeUser) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data AerospikeUserModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	adminPol := as.NewAdminPolicy()

	tmpRoles := make([]string, 0)
	for _, r := range data.Roles {
		tmpRoles = append(tmpRoles, r.ValueString())
	}

	err := (*r.asConn.client).CreateUser(adminPol, data.User_name.ValueString(), data.Password.ValueString(), tmpRoles)
	if err != nil {
		panic(err)
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "created user "+data.User_name.ValueString()+" with roles "+strings.Join(tmpRoles, ", "))

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeUser) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data AerospikeUserModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	adminPol := as.NewAdminPolicy()

	tmpRoles, err := (*r.asConn.client).QueryUser(adminPol, data.User_name.ValueString())
	if err != nil && !err.Matches(astypes.INVALID_USER) {
		panic(err)
	}

	if err != nil && err.Matches(astypes.INVALID_USER) {
		data.User_name = types.StringNull()
		data.Password = types.StringNull()
		data.Roles = nil

		tflog.Trace(ctx, "read user "+data.User_name.ValueString()+" and it does not exist")

		resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

		return
	}

	data.Roles = nil
	// Aerospike returns a one item array with "" for no roles, ignore just this case
	if len(tmpRoles.Roles) >= 1 && tmpRoles.Roles[0] != "" {
		for _, r := range tmpRoles.Roles {
			data.Roles = append(data.Roles, types.StringValue(r))
		}
	}

	tflog.Trace(ctx, "read user "+data.User_name.ValueString()+" with roles "+strings.Join(tmpRoles.Roles, ", "))

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeUser) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state, data AerospikeUserModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)

	if resp.Diagnostics.HasError() {
		return
	}

	data.User_name = plan.User_name
	data.Password = plan.Password

	if !plan.Password.Equal(state.Password) {
		adminPol := as.NewAdminPolicy()
		err := (*r.asConn.client).ChangePassword(adminPol, plan.User_name.ValueString(), plan.Password.ValueString())
		if err != nil {
			panic(err)
		}
	}

	planRoles := make([]string, 0)
	for _, r := range plan.Roles {
		planRoles = append(planRoles, r.ValueString())
	}
	sort.Strings(planRoles)

	stateRoles := make([]string, 0)
	for _, r := range state.Roles {
		stateRoles = append(stateRoles, r.ValueString())
	}
	sort.Strings(stateRoles)

	if !reflect.DeepEqual(stateRoles, planRoles) {
		// change in roles

	}

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeUser) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data AerospikeUserModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	adminPol := as.NewAdminPolicy()

	err := (*r.asConn.client).DropUser(adminPol, data.User_name.ValueString())
	if err != nil {
		panic(err)
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "dropped user "+data.User_name.ValueString())
}

func (r *AerospikeUser) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

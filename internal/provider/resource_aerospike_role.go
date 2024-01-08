// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.Resource = &AerospikeRole{}
var _ resource.ResourceWithImportState = &AerospikeRole{}

func NewAerospikeRole() resource.Resource {
	return &AerospikeRole{}
}

// AerospikeRole defines the resource implementation.
type AerospikeRole struct {
	asConn *asConnection
}

// AerospikeRoleModel describes the resource data model.
type AerospikeRoleModel struct {
	Role_name   types.String   `tfsdk:"role_name"`
	Privileges  types.String   `tfsdk:"privileges"`
	White_list  []types.String `tfsdk:"white_lost"`
	Read_quota  types.Int64    `tfsdk:"read_quota"`
	Write_quota types.Int64    `tfsdk:"write_quota"`
}

func (r *AerospikeRole) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role"
}

func (r *AerospikeRole) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		// This description is used by the documentation generator and the language server.
		MarkdownDescription: "Aerospike user",

		Attributes: map[string]schema.Attribute{
			"role_name": schema.StringAttribute{
				Description: "Role name",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"privileges": schema.SetNestedAttribute{
				Description: `Privilege set, comprised from {privilege="name",namespace="name",set="name"] maps. Namespace and Set ar optional`,
				Required:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"privilege": schema.StringAttribute{
							Description: "Privilege name",
							Required:    true,
						},
						"namespace": schema.StringAttribute{
							Description: "Namespace. Optional - if empty the privilege will apply to all namespaces",
							Optional:    true,
						},
						"set": schema.StringAttribute{
							Description: "Set. Optional - if empty the privilege will apply to all sets",
							Optional:    true,
						},
					},
				},
			},
			"white_list": schema.ListAttribute{
				Description: "A list of IP addresses allowed to connect.",
				Optional:    true,
				ElementType: types.StringType,
			},
			"read_quota": schema.Int64Attribute{
				Description: "Read quota to apply to the role",
				Optional:    true,
			},
			"write_quota": schema.Int64Attribute{
				Description: "write quota to apply to the role",
				Optional:    true,
			},
		},
	}
}

func (r *AerospikeRole) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *AerospikeRole) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {

}

func (r *AerospikeRole) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {

}

func (r *AerospikeRole) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {

}

func (r *AerospikeRole) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {

}

func (r *AerospikeRole) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("role_name"), req, resp)
}

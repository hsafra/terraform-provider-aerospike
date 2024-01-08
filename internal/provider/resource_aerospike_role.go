// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"strings"
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
	Privileges  types.Set      `tfsdk:"privileges"`
	White_list  []types.String `tfsdk:"white_list"`
	Read_quota  types.Int64    `tfsdk:"read_quota"`
	Write_quota types.Int64    `tfsdk:"write_quota"`
}

type AerospikeRolePrivilegeModel struct {
	Privilege types.String `tfsdk:"privilege"`
	Namespace types.String `tfsdk:"namespace"`
	Set       types.String `tfsdk:"set"`
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
							Validators: []validator.String{
								stringvalidator.OneOf("user-admin", "sys-admin", "data-admin", "udf-admin",
									"sindex-admin", "read-write-udf", "read-write", "read", "write", "truncate"),
							},
						},
						"namespace": schema.StringAttribute{
							Description: "Namespace. Optional - if empty the privilege will apply to all namespaces",
							Optional:    true,
						},
						"set": schema.StringAttribute{
							Description: "Set. Optional - if empty the privilege will apply to all sets",
							Optional:    true,
							//TODO: require namespace if set is set
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
	var data AerospikeRoleModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	roleName := data.Role_name.ValueString()
	readQuota := uint32(data.Read_quota.ValueInt64())
	writeQuota := uint32(data.Write_quota.ValueInt64())

	privElements := make([]types.Object, 0, len(data.Privileges.Elements()))
	data.Privileges.ElementsAs(ctx, &privElements, false)
	/*	var rrr AerospikeRolePrivilegeModel
		a := elements[0].As(ctx, &rrr, basetypes.ObjectAsOptions{})
		fmt.Println(a)
	*/
	printPrivs := make([]string, 0)
	privileges := make([]as.Privilege, 0)
	for _, p := range privElements {
		var privModel AerospikeRolePrivilegeModel
		p.As(ctx, &privModel, basetypes.ObjectAsOptions{})

		// very ugly hack since privilegeCode isn't exported and I couldn't find anything else that worked :(
		tmpPriv := as.Privilege{}
		switch privModel.Privilege.ValueString() {
		case "user-admin":
			tmpPriv = as.Privilege{as.UserAdmin, privModel.Namespace.ValueString(), privModel.Set.ValueString()}
		case "sys-admin":
			tmpPriv = as.Privilege{as.SysAdmin, privModel.Namespace.ValueString(), privModel.Set.ValueString()}
		case "data-admin":
			tmpPriv = as.Privilege{as.DataAdmin, privModel.Namespace.ValueString(), privModel.Set.ValueString()}
		case "udf-admin":
			tmpPriv = as.Privilege{as.UDFAdmin, privModel.Namespace.ValueString(), privModel.Set.ValueString()}
		case "sindex-admin":
			tmpPriv = as.Privilege{as.SIndexAdmin, privModel.Namespace.ValueString(), privModel.Set.ValueString()}
		case "read-write-udf":
			tmpPriv = as.Privilege{as.ReadWriteUDF, privModel.Namespace.ValueString(), privModel.Set.ValueString()}
		case "read":
			tmpPriv = as.Privilege{as.Read, privModel.Namespace.ValueString(), privModel.Set.ValueString()}
		case "write":
			tmpPriv = as.Privilege{as.Write, privModel.Namespace.ValueString(), privModel.Set.ValueString()}
		case "truncate":
			tmpPriv = as.Privilege{as.Truncate, privModel.Namespace.ValueString(), privModel.Set.ValueString()}
		}
		privileges = append(privileges, tmpPriv)
		printPrivs = append(printPrivs, privToStr(tmpPriv))
	}

	whiteList := make([]string, 0)
	for _, w := range data.White_list {
		whiteList = append(whiteList, w.ValueString())
	}

	adminPol := as.NewAdminPolicy()

	//TODO: check that namspaces mentioned in privileges exists

	err := (*r.asConn.client).CreateRole(adminPol, roleName, privileges, whiteList,
		readQuota, writeQuota)
	if err != nil {
		panic(err)
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "created role: "+roleName+" with privileges: "+strings.Join(printPrivs, ", ")+" whitelist: "+
		strings.Join(whiteList, ", ")+" read quota: "+string(readQuota)+" write quota:"+string(writeQuota))

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

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

func privToStr(privilege as.Privilege) string {
	return "(" + string(privilege.Code) + "," + privilege.Namespace + "," + privilege.SetName + ")"
}
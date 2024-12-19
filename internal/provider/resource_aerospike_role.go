// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64default"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"reflect"
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
		Description: "Aerospike Role",

		Attributes: map[string]schema.Attribute{
			"role_name": schema.StringAttribute{
				Description: "Role name",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"privileges": schema.SetNestedAttribute{
				Description: `Privilege set, comprised from {privilege="name",namespace="name",set="name"] maps. Namespace and Set are optional`,
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
							Description: "Namespace. Optional - if nulll the privilege will apply to all namespaces. must not be an empty string",
							Optional:    true,
							Validators: []validator.String{
								stringvalidator.LengthAtLeast(1),
							},
						},
						"set": schema.StringAttribute{
							Description: "Set. Optional - if null the privilege will apply to all sets. Must be used with namespace. Must not be an emptry string",
							Optional:    true,
							Validators: []validator.String{
								// Validate this attribute must be configured with other_attr.
								stringvalidator.AlsoRequires(path.Expressions{
									path.MatchRelative().AtParent().AtName("namespace"),
								}...),
								stringvalidator.LengthAtLeast(1),
							},
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
				Computed:    true,
				Default:     int64default.StaticInt64(0),
			},
			"write_quota": schema.Int64Attribute{
				Description: "write quota to apply to the role",
				Optional:    true,
				Computed:    true,
				Default:     int64default.StaticInt64(0),
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
	adminPol := as.NewAdminPolicy()

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
	printPrivs := make([]string, 0)
	privileges := make([]as.Privilege, 0)
	for _, p := range privElements {
		var privModel AerospikeRolePrivilegeModel
		p.As(ctx, &privModel, basetypes.ObjectAsOptions{})

		if !privModel.Namespace.IsNull() && !r.namespaceExists(privModel.Namespace.ValueString()) {
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Invalid namesace", "Namespace \""+privModel.Namespace.ValueString()+"\" does not exist in the cluster. Can't create role referencing it"))
			return
		}

		tmpPriv := asPrivFromStringValues(privModel.Privilege, privModel.Namespace, privModel.Set)
		privileges = append(privileges, tmpPriv)
		printPrivs = append(printPrivs, privToStr(tmpPriv))
	}

	whiteList := make([]string, 0)
	for _, w := range data.White_list {
		whiteList = append(whiteList, w.ValueString())
	}

	err := (*r.asConn.client).CreateRole(adminPol, roleName, privileges, whiteList,
		readQuota, writeQuota)
	if err != nil {
		switch {
		case err.Matches(astypes.QUOTAS_NOT_ENABLED):
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Quotas not enabled",
				"Role quotas are requests but not enabled in the server"))
			return
		case err.Matches(astypes.ROLE_ALREADY_EXISTS):
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Role already exists",
				"Role that was being created already exists: "+roleName))
			return
		default:
			panic(err)
		}
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "created role: "+roleName+" with privileges: "+strings.Join(printPrivs, ", ")+" whitelist: "+
		strings.Join(whiteList, ", ")+" read quota: "+fmt.Sprint(readQuota)+" write quota:"+fmt.Sprint(writeQuota))

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

}

func (r *AerospikeRole) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data AerospikeRoleModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	adminPol := as.NewAdminPolicy()

	role, err := (*r.asConn.client).QueryRole(adminPol, data.Role_name.ValueString())
	if err != nil && !err.Matches(astypes.INVALID_ROLE) {
		panic(err)
	}

	if err != nil && err.Matches(astypes.INVALID_ROLE) {
		data.Role_name = types.StringNull()
		data.Privileges = types.SetNull(privObjectType())
		data.White_list = nil
		data.Read_quota = types.Int64Null()
		data.Write_quota = types.Int64Null()

		tflog.Trace(ctx, "read role "+data.Role_name.ValueString()+" and it does not exist")

		resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

		return
	}

	if len(role.Privileges) == 0 {
		data.Privileges = types.SetNull(privObjectType())
	} else {
		privsAttrSlice := make([]attr.Value, 0)

		for _, p := range role.Privileges {
			priv, namespace, set := asPrivToStringValues(p)
			privObject, _ := types.ObjectValue(map[string]attr.Type{"privilege": types.StringType, "namespace": types.StringType, "set": types.StringType},
				map[string]attr.Value{"privilege": priv, "namespace": namespace, "set": set})
			privsAttrSlice = append(privsAttrSlice, privObject)

		}
		var diags diag.Diagnostics
		data.Privileges, diags = types.SetValue(privObjectType(), privsAttrSlice)
		if diags.HasError() {
			resp.Diagnostics = diags
			return
		}
	}

	if len(role.Whitelist) == 0 {
		data.White_list = nil
	} else {
		data.White_list = make([]types.String, 0)
		for _, w := range role.Whitelist {
			data.White_list = append(data.White_list, types.StringValue(w))
		}
	}

	data.Read_quota = types.Int64Value(int64(role.ReadQuota))
	data.Write_quota = types.Int64Value(int64(role.WriteQuota))

	tflog.Trace(ctx, "read role "+role.Name)

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

}

func (r *AerospikeRole) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state, data AerospikeRoleModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)

	if resp.Diagnostics.HasError() {
		return
	}

	adminPol := as.NewAdminPolicy()

	data.Role_name = plan.Role_name

	//privileges
	if reflect.DeepEqual(plan.Privileges, state.Privileges) {
		data.Privileges = plan.Privileges
	} else {
		planPrivElements := make([]types.Object, 0, len(data.Privileges.Elements()))
		plan.Privileges.ElementsAs(ctx, &planPrivElements, false)

		statePrivElements := make([]types.Object, 0, len(data.Privileges.Elements()))
		state.Privileges.ElementsAs(ctx, &statePrivElements, false)

		planASPrivileges := make([]as.Privilege, 0)
		for _, p := range planPrivElements {
			var privModel AerospikeRolePrivilegeModel
			p.As(ctx, &privModel, basetypes.ObjectAsOptions{})

			if !privModel.Namespace.IsNull() && !r.namespaceExists(privModel.Namespace.ValueString()) {
				resp.Diagnostics.Append(diag.NewErrorDiagnostic("Invalid namesace", "Namespace \""+privModel.Namespace.ValueString()+"\" does not exist in the cluster. Can't create role referencing it"))
				return
			}

			tmpPriv := asPrivFromStringValues(privModel.Privilege, privModel.Namespace, privModel.Set)
			planASPrivileges = append(planASPrivileges, tmpPriv)

		}

		stateASPrivileges := make([]as.Privilege, 0)
		for _, p := range statePrivElements {
			var privModel AerospikeRolePrivilegeModel
			p.As(ctx, &privModel, basetypes.ObjectAsOptions{})

			tmpPriv := asPrivFromStringValues(privModel.Privilege, privModel.Namespace, privModel.Set)
			stateASPrivileges = append(stateASPrivileges, tmpPriv)
		}

		privsToAdd := make([]as.Privilege, 0)
		for _, p := range planASPrivileges {
			if !sliceutil.Contains(stateASPrivileges, p) {
				privsToAdd = append(privsToAdd, p)
			}
		}

		privsToRevoke := make([]as.Privilege, 0)
		for _, p := range stateASPrivileges {
			if !sliceutil.Contains(planASPrivileges, p) {
				privsToRevoke = append(privsToRevoke, p)
			}
		}

		if len(privsToAdd) > 0 {
			err := (*r.asConn.client).GrantPrivileges(adminPol, plan.Role_name.ValueString(), privsToAdd)
			if err != nil {
				panic(err)
			}
		}
		if len(privsToRevoke) > 0 {
			err := (*r.asConn.client).RevokePrivileges(adminPol, plan.Role_name.ValueString(), privsToRevoke)
			if err != nil {
				panic(err)
			}
		}

		data.Privileges = plan.Privileges

	}

	//whitelist
	if !reflect.DeepEqual(plan.White_list, state.White_list) {
		whiteList := make([]string, 0)
		for _, w := range plan.White_list {
			whiteList = append(whiteList, w.ValueString())
		}
		err := (*r.asConn.client).SetWhitelist(adminPol, data.Role_name.ValueString(), whiteList)
		if err != nil {
			panic(err)
		}
	}
	data.White_list = plan.White_list

	//qoutas
	if plan.Read_quota != state.Read_quota || plan.Write_quota != state.Write_quota {
		err := (*r.asConn.client).SetQuotas(adminPol, data.Role_name.ValueString(), uint32(plan.Read_quota.ValueInt64()),
			uint32(plan.Write_quota.ValueInt64()))
		if err != nil && err.Matches(astypes.QUOTAS_NOT_ENABLED) {
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Quotas not enabled", "Role quotas are requests but not enabled in the server"))
			return
		} else if err != nil {
			panic(err)
		}
	}
	data.Read_quota = plan.Read_quota
	data.Write_quota = plan.Write_quota

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeRole) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data AerospikeRoleModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	adminPol := as.NewAdminPolicy()

	err := (*r.asConn.client).DropRole(adminPol, data.Role_name.ValueString())
	if err != nil && !err.Matches(astypes.INVALID_ROLE) {
		panic(err)
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "dropped role "+data.Role_name.ValueString())

}

func (r *AerospikeRole) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("role_name"), req, resp)
}

func privToStr(privilege as.Privilege) string {
	return "(" + string(privilege.Code) + "," + privilege.Namespace + "," + privilege.SetName + ")"
}

func (r *AerospikeRole) namespaceExists(namespace string) bool {
	key, _ := as.NewKey(namespace, "dummy", "dummy")

	_, err := (*r.asConn.client).Get(nil, key)

	return !err.Matches(astypes.INVALID_NAMESPACE)

}

func asPrivFromStringValues(priv, namespace, set types.String) as.Privilege {
	// ugly hack since privilegeCode isn't exported and I couldn't find anything else that worked :(
	var tmpPriv as.Privilege
	n := namespace.ValueString()
	s := set.ValueString()
	switch priv.ValueString() {
	case "user-admin":
		tmpPriv = as.Privilege{Code: as.UserAdmin, Namespace: n, SetName: s}
	case "sys-admin":
		tmpPriv = as.Privilege{Code: as.SysAdmin, Namespace: n, SetName: s}
	case "data-admin":
		tmpPriv = as.Privilege{Code: as.DataAdmin, Namespace: n, SetName: s}
	case "udf-admin":
		tmpPriv = as.Privilege{Code: as.UDFAdmin, Namespace: n, SetName: s}
	case "sindex-admin":
		tmpPriv = as.Privilege{Code: as.SIndexAdmin, Namespace: n, SetName: s}
	case "read-write-udf":
		tmpPriv = as.Privilege{Code: as.ReadWriteUDF, Namespace: n, SetName: s}
	case "read":
		tmpPriv = as.Privilege{Code: as.Read, Namespace: n, SetName: s}
	case "write":
		tmpPriv = as.Privilege{Code: as.Write, Namespace: n, SetName: s}
	case "read-write":
		tmpPriv = as.Privilege{Code: as.ReadWrite, Namespace: n, SetName: s}
	case "truncate":
		tmpPriv = as.Privilege{Code: as.Truncate, Namespace: n, SetName: s}
	}
	return tmpPriv
}

func asPrivToStringValues(priv as.Privilege) (types.String, types.String, types.String) {
	var code string
	var namespace, set types.String
	switch priv.Code {
	case as.UserAdmin:
		code = "user-admin"
	case as.SysAdmin:
		code = "sys-admin"
	case as.DataAdmin:
		code = "data-admin"
	case as.UDFAdmin:
		code = "udf-admin"
	case as.SIndexAdmin:
		code = "sindex-admin"
	case as.ReadWriteUDF:
		code = "read-write-udf"
	case as.Read:
		code = "read"
	case as.Write:
		code = "write"
	case as.ReadWrite:
		code = "read-write"
	case as.Truncate:
		code = "truncate"
	}

	if priv.Namespace == "" {
		namespace = types.StringNull()
	} else {
		namespace = types.StringValue(priv.Namespace)
	}
	if priv.SetName == "" {
		set = types.StringNull()
	} else {
		set = types.StringValue(priv.SetName)
	}

	return types.StringValue(code), namespace, set
}

func privObjectType() types.ObjectType {
	return types.ObjectType{AttrTypes: map[string]attr.Type{"privilege": types.StringType, "namespace": types.StringType, "set": types.StringType}}
}

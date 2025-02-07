// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
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
	"github.com/onsi/gomega/matchers/support/goraph/node"
	"reflect"
	"strings"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.Resource = &AerospikeNamespaceConfig{}
var _ resource.ResourceWithImportState = &AerospikeNamespaceConfig{}

func NewAerospikeNamespaceConfig() resource.Resource {
	return &AerospikeNamespaceConfig{}
}

// AerospikeNamespaceConfig defines the resource implementation.
type AerospikeNamespaceConfig struct {
	asConn *asConnection
}

// AerospikeNamespaceConfigModel describes the resource data model.
type AerospikeNamespaceConfigModel struct {
	Namespace         types.String   `tfsdk:"namespace"`
	Default_set_ttl   types.Map      `tfsdk:"default_set_ttl"`
	XDR_include       []types.String `tfsdk:"xdr_include"`
	XDR_exclude       []types.String `tfsdk:"xdr_exclude"`
	Migartion_threads types.Int64    `tfsdk:"migartion_threads"`
}

func (r *AerospikeNamespaceConfig) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namespace_config"
}

func (r *AerospikeNamespaceConfig) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		// This description is used by the documentation generator and the language server.
		Description: "Aerospike Namespace Configuration",

		Attributes: map[string]schema.Attribute{
			"namespace": schema.StringAttribute{
				Description: "Namespace name",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"default_set_ttl": schema.MapAttribute{
				Description: "Default TTL for sets in the namespace",
				Optional:    true,
				ElementType: types.StringType,
			},
			"xdr_include": schema.ListAttribute{
				Description: "A list of sets to include in XDR",
				Optional:    true,
				ElementType: types.StringType,
				Validators: []validator.List{
					listvalidator.ConflictsWith(path.MatchRoot("xdr_exclude")),
				},
			},
			"xdr_exclude": schema.ListAttribute{
				Description: "A list of sets to exclude from XDR",
				Optional:    true,
				ElementType: types.StringType,
				Validators: []validator.List{
					listvalidator.ConflictsWith(path.MatchRoot("xdr_include")),
				},
			},
			"migartion_threads": schema.Int64Attribute{
				Description: "The number of migration threads to use for the namespace",
				Optional:    true,
			},
		},
	}
}

func (r *AerospikeNamespaceConfig) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *AerospikeNamespaceConfig) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data AerospikeNamespaceConfigModel
	infoPol := as.NewInfoPolicy()

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	namespace := data.Namespace.ValueString()

	if !data.Default_set_ttl.IsNull() {

		// copy the map to a go map
		ttlMap := make(map[string]types.String, len(data.Default_set_ttl.Elements()))

		randomNode, err := (*r.asConn.client).Cluster().GetRandomNode()
		if err != nil {
			panic(err)
		}

		// iterate over the map and set the default ttl in Aerospike
		for set, ttl := range ttlMap {
			command := "set-config:context=namespace;id=" + namespace + ";set=" + set + ";default-ttl=" + ttl.ValueString()
			_, err := randomNode.RequestInfo(infoPol, command)
			if err != nil {
				panic(err)
			}
		}

	}

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

func (r *AerospikeNamespaceConfig) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
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

func (r *AerospikeNamespaceConfig) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
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

			if !privModel.Namespace.IsNull() && !namespaceExists(*r.asConn.client, privModel.Namespace.ValueString()) {
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

func (r *AerospikeNamespaceConfig) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
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

func (r *AerospikeNamespaceConfig) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("role_name"), req, resp)
}

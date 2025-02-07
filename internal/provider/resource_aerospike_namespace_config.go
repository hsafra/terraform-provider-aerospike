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
	"strconv"
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

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	namespace := data.Namespace.ValueString()

	if !data.Default_set_ttl.IsNull() {
		if !supportsCapability(*r.asConn.client, SetLevelTTL) {
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Invalid server vesrion", "Aerospike server version does not support set level ttl. Versions "+strconv.Itoa(int(SetLevelTTL))+" are required"))
			return
		}

		// copy the map to a go map
		ttlMap := make(map[string]types.String, len(data.Default_set_ttl.Elements()))

		// iterate over the map and set the default ttl in Aerospike
		for set, ttl := range ttlMap {
			command := "set-config:context=namespace;id=" + namespace + ";set=" + set + ";default-ttl=" + ttl.ValueString()
			_, err := sendInfoCommand(*r.asConn.client, command)
			if err != nil {
				panic(err)
			}
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

	// Write logs using the tflog package
	tflog.Trace(ctx, "dropped role "+data.Role_name.ValueString())

}

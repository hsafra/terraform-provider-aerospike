// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
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
	"strconv"
	"strings"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.Resource = &AerospikeNamespaceConfig{}

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
	XDR_datacenter    types.String   `tfsdk:"xdr_datacenter"`
	XDR_include       []types.String `tfsdk:"xdr_include"`
	XDR_exclude       []types.String `tfsdk:"xdr_exclude"`
	Migartion_threads types.Int64    `tfsdk:"migartion_threads"`
	Info_commands     types.List     `tfsdk:"info_commands"`
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
			"xdr_datacenter": schema.StringAttribute{
				Description: "The XDR datacenter to use for the namespace. Must be specified with xdr_include or xdr_exclude",
				Optional:    true,
				Validators: []validator.String{
					stringvalidator.Any(
						stringvalidator.AlsoRequires(path.MatchRoot("xdr_exclude")),
						stringvalidator.AlsoRequires(path.MatchRoot("xdr_include")),
					),
				},
			},
			"xdr_include": schema.ListAttribute{
				Description: "A list of sets to include in XDR. Don't use along with xdr_exclude, must be specified with xdr_datacenter",
				Optional:    true,
				ElementType: types.StringType,
				Validators: []validator.List{
					listvalidator.ConflictsWith(path.MatchRoot("xdr_exclude")),
				},
			},
			"xdr_exclude": schema.ListAttribute{
				Description: "A list of sets to exclude from XDR. Don't use along with xdr_include, must be specified with xdr_datacenter",
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
			"info_commands": schema.ListAttribute{
				Description: "An output only list of asinfo compatible commands that were run",
				ElementType: types.StringType,
				Computed:    true,
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
	var diags diag.Diagnostics

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	namespace := data.Namespace.ValueString()

	if !data.Default_set_ttl.IsNull() {
		supported, err := supportsCapability(*r.asConn.client, SetLevelTTL)
		if err != nil {
			panic(err)
		}

		if !supported {
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

			data.Info_commands, diags = appendStringToListString(command, data.Info_commands)
			if diags.HasError() {
				resp.Diagnostics = diags
				return
			}
		}
	}

	if len(data.XDR_exclude) > 0 {
		command := "set-config:context=xdr;dc=" + data.XDR_datacenter.ValueString() + ";namespace=" + namespace + ";ship-only-specified-sets=false"
		_, err := sendInfoCommand(*r.asConn.client, command)
		if err != nil {
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error in request", "Error in request: "+err.Error()))
			return
		}
		tflog.Trace(ctx, "Applied namespace config to "+namespace+" with command "+command)
		data.Info_commands, diags = appendStringToListString(command, data.Info_commands)

		//Admin+> asinfo -v "set-config:context=xdr;dc=dc2;namespace=example;ignore-sets=set1"

		sets := make([]string, 0, len(data.XDR_exclude))
		for _, set := range data.XDR_exclude {
			sets = append(sets, set.ValueString())
		}

		command = "set-config:context=xdr;dc=" + data.XDR_datacenter.ValueString() + ";namespace=" + namespace + ";ignore-set=" + strings.Join(sets, ",")

		_, err = sendInfoCommand(*r.asConn.client, command)
		if err != nil {
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error in request", "Error in request: "+err.Error()))
			return
		}
		tflog.Trace(ctx, "Applied namespace config to "+namespace+" with command "+command)
		data.Info_commands, diags = appendStringToListString(command, data.Info_commands)
	}

	if len(data.XDR_include) > 0 {
		command := "set-config:context=xdr;dc=" + data.XDR_datacenter.ValueString() + ";namespace=" + namespace + ";ship-only-specified-sets=true"
		_, err := sendInfoCommand(*r.asConn.client, command)
		if err != nil {
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error in request", "Error in request: "+err.Error()))
			return
		}
		tflog.Trace(ctx, "Applied namespace config to "+namespace+" with command "+command)
		data.Info_commands, diags = appendStringToListString(command, data.Info_commands)

		//Admin+> asinfo -v "set-config:context=xdr;dc=dc2;namespace=example;ship-set=set1"

		sets := make([]string, 0, len(data.XDR_include))
		for _, set := range data.XDR_include {
			sets = append(sets, set.ValueString())
		}

		command = "set-config:context=xdr;dc=" + data.XDR_datacenter.ValueString() + ";namespace=" + namespace + ";ship-set=" + strings.Join(sets, ",")

		_, err = sendInfoCommand(*r.asConn.client, command)
		if err != nil {
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error in request", "Error in request: "+err.Error()))
			return
		}
		tflog.Trace(ctx, "Applied namespace config to "+namespace+" with command "+command)
		data.Info_commands, diags = appendStringToListString(command, data.Info_commands)
	}

	if !data.Migartion_threads.IsNull() {
		//asinfo -v "set-config:context=service;migrate-threads=0"

		command := "set-config:context=service;migrate-threads=" + strconv.Itoa(int(data.Migartion_threads.ValueInt64()))
		_, err := sendInfoCommand(*r.asConn.client, command)
		if err != nil {
			panic(err)
		}
		data.Info_commands, diags = appendStringToListString(command, data.Info_commands)
		if diags.HasError() {
			resp.Diagnostics = diags
			return
		}
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "Applied namespace config to "+namespace)

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

}

func (r *AerospikeNamespaceConfig) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data AerospikeNamespaceConfigModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Trace(ctx, "read namespace config for namespace "+data.Namespace.ValueString())

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

}

func (r *AerospikeNamespaceConfig) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state, data AerospikeNamespaceConfigModel

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
	var data AerospikeNamespaceConfigModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "Deleted namespace config for "+data.Namespace.ValueString())

}

// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"strconv"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.Resource = &AerospikeConfigService{}

func NewAerospikeConfigService() resource.Resource {
	return &AerospikeConfigService{}
}

// AerospikeNamespaceConfig defines the resource implementation.
type AerospikeConfigService struct {
	asConn *asConnection
}

// AerospikeNamespaceConfigModel describes the resource data model.
type AerospikeConfigServiceModel struct {
	Migartion_threads types.Int64 `tfsdk:"migartion_threads"`
	Info_commands     types.List  `tfsdk:"info_commands"`
}

func (r *AerospikeConfigService) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_config_service"
}

func (r *AerospikeConfigService) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		// This description is used by the documentation generator and the language server.
		Description: "Aerospike Service Configuration",

		Attributes: map[string]schema.Attribute{
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

func (r *AerospikeConfigService) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *AerospikeConfigService) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data AerospikeConfigServiceModel
	var diags diag.Diagnostics

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
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
	tflog.Trace(ctx, "Applied service config")

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

}

func (r *AerospikeConfigService) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data AerospikeConfigServiceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Trace(ctx, "read service config")

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

}

func (r *AerospikeConfigService) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state, data AerospikeConfigServiceModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)

	if resp.Diagnostics.HasError() {
		return
	}

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeConfigService) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data AerospikeConfigServiceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "Deleted service config")

}

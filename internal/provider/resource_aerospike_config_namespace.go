// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
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
var _ resource.Resource = &AerospikeConfigNamespace{}

func NewAerospikeConfigNamespace() resource.Resource {
	return &AerospikeConfigNamespace{}
}

// AerospikeNamespaceConfig defines the resource implementation.
type AerospikeConfigNamespace struct {
	asConn *asConnection
}

// AerospikeNamespaceConfigModel describes the resource data model.
type AerospikeConfigNamespaceModel struct {
	Namespace       types.String `tfsdk:"namespace"`
	Default_set_ttl types.Map    `tfsdk:"default_set_ttl"`
	Info_commands   types.List   `tfsdk:"info_commands"`
	XDR_config      types.Object `tfsdk:"xdr_config"`
}

type AerospikeConfigNamespaceXDRModel struct {
	Datacenter               types.String   `tfsdk:"datacenter"`
	Ship_only_specified_sets types.Bool     `tfsdk:"ship_only_specified_sets"`
	Include_sets             []types.String `tfsdk:"include_sets"`
	Exclude_sets             []types.String `tfsdk:"exclude_sets"`
}

func (r *AerospikeConfigNamespace) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_config_namespace"
}

func (r *AerospikeConfigNamespace) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
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
			"info_commands": schema.ListAttribute{
				Description: "An output only list of asinfo compatible commands that were run",
				ElementType: types.StringType,
				Computed:    true,
			},
			"xdr_config": schema.SingleNestedAttribute{
				Description: "Aerospike Namespace Configuration",
				Optional:    true,
				Attributes: map[string]schema.Attribute{
					"datacenter": schema.StringAttribute{
						Description: "The XDR datacenter to use for the namespace. Must be specified with xdr_include or xdr_exclude",
						Required:    true,
					},
					"ship_only_specified_sets": schema.BoolAttribute{
						Description: "If true, only the sets specified in xdr_include will be shipped to the XDR datacenter. If false, all sets except those specified in xdr_exclude will be shipped",
						Required:    true,
					},
					"include_sets": schema.ListAttribute{
						Description: "A list of sets to include in XDR. Don't use along with exclude_sets",
						Optional:    true,
						ElementType: types.StringType,
						Validators: []validator.List{
							listvalidator.ConflictsWith(path.MatchRelative().AtParent().AtName("exclude_sets")),
						},
					},
					"exclude_sets": schema.ListAttribute{
						Description: "A list of sets to exclude from XDR. Don't use along with include_sets",
						Optional:    true,
						ElementType: types.StringType,
						Validators: []validator.List{
							listvalidator.ConflictsWith(path.MatchRelative().AtParent().AtName("include_sets")),
						},
					},
				},
			},
		},
	}
}

func (r *AerospikeConfigNamespace) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *AerospikeConfigNamespace) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data AerospikeConfigNamespaceModel
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

		// iterate over the map and set the default ttl in Aerospike
		for set, ttl := range data.Default_set_ttl.Elements() {
			command := "set-config:context=namespace;id=" + namespace + ";set=" + set + ";default-ttl=" + ttl.String()
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

	if !data.XDR_config.IsNull() {
		var xdrConfig AerospikeConfigNamespaceXDRModel

		diags = req.Plan.GetAttribute(ctx, path.Root("xdr_config"), &xdrConfig)

		resp.Diagnostics.Append(diags...)

		if resp.Diagnostics.HasError() {
			return
		}

		if xdrConfig.Ship_only_specified_sets.ValueBool() {
			command := "set-config:context=xdr;dc=" + xdrConfig.Datacenter.ValueString() + ";namespace=" + namespace + ";ship-only-specified-sets=true"
			_, err := sendInfoCommand(*r.asConn.client, command)
			if err != nil {
				resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error in request", "Error in request: "+err.Error()))
				return
			}
			tflog.Trace(ctx, "Applied namespace config to "+namespace+" with command "+command)
			data.Info_commands, diags = appendStringToListString(command, data.Info_commands)

			//Admin+> asinfo -v "set-config:context=xdr;dc=dc2;namespace=example;ship-set=set1"

			sets := make([]string, 0, len(xdrConfig.Include_sets))
			for _, set := range xdrConfig.Include_sets {
				sets = append(sets, set.ValueString())
			}

			command = "set-config:context=xdr;dc=" + xdrConfig.Datacenter.ValueString() + ";namespace=" + namespace + ";ship-set=" + strings.Join(sets, ",")

			_, err = sendInfoCommand(*r.asConn.client, command)
			if err != nil {
				resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error in request", "Error in request: "+err.Error()))
				return
			}
			tflog.Trace(ctx, "Applied namespace config to "+namespace+" with command "+command)
			data.Info_commands, diags = appendStringToListString(command, data.Info_commands)
		} else {
			command := "set-config:context=xdr;dc=" + xdrConfig.Datacenter.ValueString() + ";namespace=" + namespace + ";ship-only-specified-sets=false"
			_, err := sendInfoCommand(*r.asConn.client, command)
			if err != nil {
				resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error in request", "Error in request: "+err.Error()))
				return
			}
			tflog.Trace(ctx, "Applied namespace config to "+namespace+" with command "+command)
			data.Info_commands, diags = appendStringToListString(command, data.Info_commands)

			//Admin+> asinfo -v "set-config:context=xdr;dc=dc2;namespace=example;ship-set=set1"

			sets := make([]string, 0, len(xdrConfig.Exclude_sets))
			for _, set := range xdrConfig.Exclude_sets {
				sets = append(sets, set.ValueString())
			}

			command = "set-config:context=xdr;dc=" + xdrConfig.Datacenter.ValueString() + ";namespace=" + namespace + ";ignore-set=" + strings.Join(sets, ",")

			_, err = sendInfoCommand(*r.asConn.client, command)
			if err != nil {
				resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error in request", "Error in request: "+err.Error()))
				return
			}
			tflog.Trace(ctx, "Applied namespace config to "+namespace+" with command "+command)
			data.Info_commands, diags = appendStringToListString(command, data.Info_commands)
		}
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "Applied namespace config to "+namespace)

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

}

func (r *AerospikeConfigNamespace) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data AerospikeConfigNamespaceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	if !data.XDR_config.IsNull() {
		// Read ship-only-specified-sets
		data.XDR_config.
			command := "get-config:context=xdr;dc=" + data.XDR_config.Attrs["datacenter"].(types.String).ValueString() + ";namespace=" + data.Namespace.ValueString() + ";ship-only-specified-sets"

	}
	tflog.Trace(ctx, "read namespace config for namespace "+data.Namespace.ValueString())

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

}

func (r *AerospikeConfigNamespace) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state, data AerospikeConfigNamespaceModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)

	if resp.Diagnostics.HasError() {
		return
	}

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *AerospikeConfigNamespace) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data AerospikeConfigNamespaceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	// Write logs using the tflog package
	tflog.Trace(ctx, "Deleted namespace config for "+data.Namespace.ValueString())

}

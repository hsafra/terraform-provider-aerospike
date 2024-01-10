// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"time"
)

// Ensure AerospikeProvider satisfies various provider interfaces.
var _ provider.Provider = &AerospikeProvider{}

// AerospikeProvider defines the provider implementation.
type AerospikeProvider struct {
	// version is set to the provider version on release, "dev" when the
	// provider is built and ran locally, and "test" when running acceptance
	// testing.
	version string
}

// AerospikeProviderModel describes the provider data model.
type AerospikeProviderModel struct {
	Host     types.String `tfsdk:"host"`
	Port     types.Int64  `tfsdk:"port"`
	UserName types.String `tfsdk:"user_name"`
	Password types.String `tfsdk:"password"`
	//tlsEnabled  types.Bool   `tfsdk:"tls_enabled"`
	//tlsName     types.String `tfsdk:"tls_name"`
	//tlsCertPath types.String `tfsdk:"tls_cert"`
}

type asConnection struct {
	client *as.ClientIfc
}

func (p *AerospikeProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "aerospike"
	resp.Version = p.version
}

func (p *AerospikeProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"host": schema.StringAttribute{
				Description: "Seed host to connect to. Defaults to localhost",
				Optional:    true,
			},
			"port": schema.Int64Attribute{
				Description: "Port to connect to. Defaults to 3000",
				Optional:    true,
				Validators: []validator.Int64{
					int64validator.Between(0, 65535),
				},
			},
			"user_name": schema.StringAttribute{
				Description: "Admin username. Defaults to admin",
				Optional:    true,
			},
			"password": schema.StringAttribute{
				Description: "Admin password. Defaults to admin",
				Optional:    true,
				Sensitive:   true,
			},
			//"tls": schema.SingleNestedAttribute{
			//	Attributes: map[string]schema.Attribute{
			//		"enabled": schema.BoolAttribute{
			//			Description: "Use tls?",
			//			Optional:    true,
			//		},
			//		"name": schema.StringAttribute{
			//			Description: "tls name to use",
			//			Optional:    true,
			//		},
			//		"cert_path": schema.StringAttribute{
			//			Description: "tls certificate path",
			//			Optional:    true,
			//		},
			//	},
			//	Optional: true,
			//},
		},
	}
}

func (p *AerospikeProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data AerospikeProviderModel
	var err error
	var asConn asConnection
	var tempConn as.ClientIfc

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	user := withEnvironmentOverrideString(data.UserName.ValueString(), "AEROSPIKE_USER")
	password := withEnvironmentOverrideString(data.Password.ValueString(), "AEROSPIKE_PASSWORD")
	host := withEnvironmentOverrideString(data.Host.ValueString(), "AEROSPIKE_HOST")
	port := withEnvironmentOverrideInt64(data.Port.ValueInt64(), "AEROSPIKE_PORT")

	cp := as.NewClientPolicy()
	cp.User = user
	cp.Password = password
	cp.Timeout = 3 * time.Second

	ash := as.NewHost(host, int(port))
	tempConn, err = as.CreateClientWithPolicyAndHost(as.CTNative, cp, ash)
	if err != nil {
		panic(err)
	}

	asConn.client = &tempConn

	resp.DataSourceData = &asConn
	resp.ResourceData = &asConn
}

func (p *AerospikeProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewAerospikeUser,
		NewAerospikeRole,
	}
}

func (p *AerospikeProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &AerospikeProvider{
			version: version,
		}
	}
}

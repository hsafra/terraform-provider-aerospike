// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
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
	seedHost    types.String `tfsdk:"seed_host"`
	port        types.Int64  `tfsdk:"port"`
	userName    types.String `tfsdk:"user_name"`
	password    types.String `tfsdk:"password"`
	tlsEnabled  types.Bool   `tfsdk:"tls_enabled"`
	tlsName     types.String `tfsdk:"tls_name"`
	tlsCertPath types.String `tfsdk:"tls_cert"`
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
				Description: "Seed host to connect to",
				Required:    true,
			},
			"port": schema.Int64Attribute{
				Description: "Port to connect to",
				Required:    true,
			},
			"user_name": schema.StringAttribute{
				Description: "Admin username",
				Required:    true,
			},
			"password": schema.StringAttribute{
				Description: "Admin password",
				Required:    true,
				Sensitive:   true,
			},
			"tls": schema.SingleNestedAttribute{
				Attributes: map[string]schema.Attribute{
					"enabled": schema.BoolAttribute{
						Description: "Use tls?",
						Optional:    true,
					},
					"name": schema.StringAttribute{
						Description: "tls name to use",
						Optional:    true,
					},
					"cert_path": schema.StringAttribute{
						Description: "tls certificate path",
						Optional:    true,
					},
				},
				Optional: true,
			},
		},
	}
}

func (p *AerospikeProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data AerospikeProviderModel
	var err error
	var asConn asConnection

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	// Configuration values are now available.
	// if data.Endpoint.IsNull() { /* ... */ }

	// Example client configuration for data sources and resources

	user := "admin"
	password := "admin"
	host := "127.0.0.1"
	port := 3000

	cp := as.NewClientPolicy()
	cp.User = user
	cp.Password = password
	cp.Timeout = 3 * time.Second

	ash := as.NewHost(host, port)
	*asConn.client, err = as.CreateClientWithPolicyAndHost(as.CTNative, cp, ash)
	if err != nil {
		panic(err)
	}

	resp.DataSourceData = asConn
	resp.ResourceData = asConn
}

func (p *AerospikeProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewExampleResource,
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

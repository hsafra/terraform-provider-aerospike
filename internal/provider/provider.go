// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	as "github.com/aerospike/aerospike-client-go/v6"
	astypes "github.com/aerospike/aerospike-client-go/v6/types"
	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"io"
	"os"
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
	Host            types.String `tfsdk:"host"`
	Port            types.Int64  `tfsdk:"port"`
	User_name       types.String `tfsdk:"user_name"`
	Password        types.String `tfsdk:"password"`
	Connect_timeout types.Int64  `tfsdk:"connect_timeout"`
	TLS             types.Object `tfsdk:"tls"`
}

type AerospikeTLSConfigModel struct {
	TLSName    types.String `tfsdk:"tls_name"`
	RootCAFile types.String `tfsdk:"root_ca_file"`
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
				Description: "Seed host to connect to. Defaults to the environment variable AEROSPIKE_HOST",
				Optional:    true,
			},
			"port": schema.Int64Attribute{
				Description: "Port to connect to. Defaults to the environment variable AEROSPIKE_PORT",
				Optional:    true,
				Validators: []validator.Int64{
					int64validator.Between(0, 65535),
				},
			},
			"user_name": schema.StringAttribute{
				Description: "Admin username. Defaults to the environment variable AEROSPIKE_USER",
				Optional:    true,
			},
			"password": schema.StringAttribute{
				Description: "Admin password. Defaults to the environment variable AEROSPIKE_PASSWORD",
				Optional:    true,
				Sensitive:   true,
			},
			"connect_timeout": schema.Int64Attribute{
				Description: "Connect timeout. Defaults to the environment variable AEROSPIKE_CONNECT_TIMEOUT. Range is 1-60 seconds",
				Optional:    true,
				Validators: []validator.Int64{
					int64validator.Between(0, 60),
				},
			},
			"tls": schema.SingleNestedAttribute{
				Attributes: map[string]schema.Attribute{
					"tls_name": schema.StringAttribute{
						Description: "tls name to use",
						Optional:    true,
					},
					"root_ca_file": schema.StringAttribute{
						Description: "root CA tls certificate file",
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
	var dataTLS AerospikeTLSConfigModel
	var err as.Error
	var asConn asConnection
	var tempConn as.ClientIfc

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	user := withEnvironmentOverrideString(data.User_name.ValueString(), "AEROSPIKE_USER")
	password := withEnvironmentOverrideString(data.Password.ValueString(), "AEROSPIKE_PASSWORD")
	host := withEnvironmentOverrideString(data.Host.ValueString(), "AEROSPIKE_HOST")
	port := withEnvironmentOverrideInt64(data.Port.ValueInt64(), "AEROSPIKE_PORT")
	connectTimeout := withEnvironmentOverrideInt64(data.Connect_timeout.ValueInt64(), "AEROSPIKE_CONNECT_TIMEOUT")

	cp := as.NewClientPolicy()
	cp.User = user
	cp.Password = password
	if connectTimeout != 0 {
		cp.Timeout = time.Second * time.Duration(connectTimeout)
	}

	//TLS
	var tlsEnabled bool
	var tlsConfig tls.Config

	if data.TLS.IsNull() {
		tlsEnabled = false
	} else {
		tlsEnabled = true
		data.TLS.As(ctx, &dataTLS, basetypes.ObjectAsOptions{})

		//read the root ca if supplied
		if !dataTLS.RootCAFile.IsNull() {
			file, err := os.Open(dataTLS.RootCAFile.ValueString())
			if err != nil {
				resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error reading root ca file", err.Error()))
				return
			}
			defer file.Close()

			// Get the file size
			stat, err := file.Stat()
			if err != nil {
				resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error reading root ca file", err.Error()))
				return
			}

			// Read the file into a byte slice
			bs := make([]byte, stat.Size())
			_, err = bufio.NewReader(file).Read(bs)
			if err != nil && err != io.EOF {
				resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error reading root ca file", err.Error()))
				return
			}

			roots := x509.NewCertPool()
			ok := roots.AppendCertsFromPEM(bs)
			if !ok {
				resp.Diagnostics.Append(diag.NewErrorDiagnostic("Error reading root ca file", err.Error()))
				return
			}
			tlsConfig.RootCAs = roots
		}
	}

	ash := as.NewHost(host, int(port))
	if tlsEnabled {
		if !dataTLS.TLSName.IsNull() {
			ash.TLSName = dataTLS.TLSName.ValueString()
		}
		cp.TlsConfig = &tlsConfig
	}
	tempConn, err = as.CreateClientWithPolicyAndHost(as.CTNative, cp, ash)
	if err != nil {
		if err.Matches(astypes.TIMEOUT) {
			resp.Diagnostics.Append(diag.NewErrorDiagnostic("Timeout connecting to Aerospike",
				"Timeout connecting to Aerospike cluster "+host+" "+err.Error()))
			return
		} else {
			panic(err)
		}
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

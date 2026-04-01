// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

// testAccProtoV6ProviderFactories are used to instantiate a provider during
// acceptance testing. The factory function will be invoked for every Terraform
// CLI command executed to create a provider server to which the CLI can
// reattach.
var testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"aerospike": providerserver.NewProtocol6WithError(New("test")()),
}

func testAccPreCheck(t *testing.T) {
	// Verify we can connect to the Aerospike cluster before running tests
	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to connect to Aerospike for acceptance tests: %s", err)
	}
	(*client).Close()
}

// testAccGetAerospikeClient returns an Aerospike client using the same
// connection parameters as the provider (env vars or defaults from the Makefile).
func testAccGetAerospikeClient() (*as.ClientIfc, error) {
	host := withEnvironmentOverrideString("localhost", "AEROSPIKE_HOST")
	port := withEnvironmentOverrideInt64(3000, "AEROSPIKE_PORT")
	user := withEnvironmentOverrideString("admin", "AEROSPIKE_USER")
	password := withEnvironmentOverrideString("admin", "AEROSPIKE_PASSWORD")

	cp := as.NewClientPolicy()
	cp.User = user
	cp.Password = password

	client, err := as.CreateClientWithPolicyAndHost(as.CTNative, cp, as.NewHost(host, int(port)))
	if err != nil {
		return nil, fmt.Errorf("failed to create Aerospike client: %w", err)
	}

	return &client, nil
}

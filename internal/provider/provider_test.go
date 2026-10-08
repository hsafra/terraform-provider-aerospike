// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
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
	client.Close()
}

// testAccGetAerospikeClient returns an Aerospike client using the same
// connection parameters as the provider (env vars or defaults from the Makefile).
func testAccGetAerospikeClient() (*as.Client, error) {
	host := withEnvironmentOverrideString("localhost", "AEROSPIKE_HOST")
	port := withEnvironmentOverrideInt64(3000, "AEROSPIKE_PORT")
	user := withEnvironmentOverrideString("admin", "AEROSPIKE_USER")
	password := withEnvironmentOverrideString("admin", "AEROSPIKE_PASSWORD")

	cp := as.NewClientPolicy()
	cp.User = user
	cp.Password = password
	cp.UseServicesAlternate = true

	client, err := as.NewClientWithPolicyAndHost(cp, as.NewHost(host, int(port)))
	if err != nil {
		return nil, fmt.Errorf("failed to create Aerospike client: %w", err)
	}

	return client, nil
}

// Security changes (users, roles) reach the other cluster nodes asynchronously,
// so a query right after a drop can land on a node that still has the object.
const (
	testAccSecurityPropagationTimeout  = 10 * time.Second
	testAccSecurityPropagationInterval = 250 * time.Millisecond
)

// testAccEventually calls check until it returns nil or timeout elapses, and
// returns the last error.
func testAccEventually(timeout, interval time.Duration, check func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := check()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(interval)
	}
}

// testAccCheckRoleGone waits until roleName is gone from the cluster.
func testAccCheckRoleGone(client *as.Client, roleName string) error {
	return testAccEventually(testAccSecurityPropagationTimeout, testAccSecurityPropagationInterval, func() error {
		_, queryErr := client.QueryRole(as.NewAdminPolicy(), roleName)
		if queryErr == nil {
			return fmt.Errorf("aerospike role %s still exists", roleName)
		}
		if !queryErr.Matches(astypes.INVALID_ROLE) {
			return fmt.Errorf("unexpected error checking role %s: %w", roleName, queryErr)
		}
		return nil
	})
}

// testAccCheckUserGone waits until userName is gone from the cluster.
func testAccCheckUserGone(client *as.Client, userName string) error {
	return testAccEventually(testAccSecurityPropagationTimeout, testAccSecurityPropagationInterval, func() error {
		_, queryErr := client.QueryUser(as.NewAdminPolicy(), userName)
		if queryErr == nil {
			return fmt.Errorf("aerospike user %s still exists", userName)
		}
		if !queryErr.Matches(astypes.INVALID_USER) {
			return fmt.Errorf("unexpected error checking user %s: %w", userName, queryErr)
		}
		return nil
	})
}

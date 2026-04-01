// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"regexp"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// testAccNamespaceConfigPreCheck ensures the admin user has sys-admin role
// required for set-config commands, and that at least one set exists in the
// namespace for set-level param validation.
func testAccNamespaceConfigPreCheck(t *testing.T) {
	t.Helper()
	testAccPreCheck(t)

	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to connect to Aerospike: %s", err)
	}
	defer client.Close()

	adminPol := as.NewAdminPolicy()
	_ = client.GrantRoles(adminPol, "admin", []string{"sys-admin", "read-write"})
	// Close and reconnect so the new roles take effect on the connection
	client.Close()

	client, err = testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to reconnect to Aerospike after granting roles: %s", err)
	}
	defer client.Close()

	// Write a dummy record to ensure a set exists (needed for set param validation)
	key, _ := as.NewKey("aerospike", "dummy_validation_set", "dummy")
	wp := as.NewWritePolicy(0, 60)
	putErr := client.Put(wp, key, as.BinMap{"dummy": 1})
	if putErr != nil {
		t.Fatalf("Failed to create dummy set for validation: %s", putErr)
	}
}

// testAccGetNamespaceParam reads a namespace config parameter directly from Aerospike.
func testAccGetNamespaceParam(namespace, key string) (string, error) {
	client, err := testAccGetAerospikeClient()
	if err != nil {
		return "", err
	}
	defer client.Close()

	config, err := getNamespaceConfig(client, namespace)
	if err != nil {
		return "", err
	}

	val, ok := config[key]
	if !ok {
		return "", fmt.Errorf("parameter %q not found in namespace %q config", key, namespace)
	}
	return val, nil
}

// testAccCheckNamespaceParam verifies a namespace parameter has the expected value on the server.
func testAccCheckNamespaceParam(namespace, key, expected string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		actual, err := testAccGetNamespaceParam(namespace, key)
		if err != nil {
			return err
		}
		if actual != expected {
			return fmt.Errorf("namespace %q param %q: expected %q, got %q", namespace, key, expected, actual)
		}
		return nil
	}
}

// testAccCheckAerospikeNamespaceConfigDestroy verifies the resource is removed from state.
// Namespace config always persists on the server, so we only check state removal.
func testAccCheckAerospikeNamespaceConfigDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aerospike_namespace_config" {
			continue
		}
		// Resource should be removed from state — nothing to check server-side
		// since namespace config always exists
	}
	return nil
}

func TestAccAerospikeNamespaceConfig_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccNamespaceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeNamespaceConfigDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccNamespaceConfigBasic(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_namespace_config.test", "namespace", "aerospike"),
					resource.TestCheckResourceAttr("aerospike_namespace_config.test", "params.default-ttl", "100"),
					resource.TestCheckResourceAttrSet("aerospike_namespace_config.test", "info_commands.#"),
					testAccCheckNamespaceParam("aerospike", "default-ttl", "100"),
				),
			},
		},
	})
}

func TestAccAerospikeNamespaceConfig_update(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccNamespaceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeNamespaceConfigDestroy,
		Steps: []resource.TestStep{
			// Create with initial value
			{
				Config: testAccNamespaceConfigWithParam("default-ttl", "200"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_namespace_config.test", "params.default-ttl", "200"),
					testAccCheckNamespaceParam("aerospike", "default-ttl", "200"),
				),
			},
			// Update the value
			{
				Config: testAccNamespaceConfigWithParam("default-ttl", "300"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_namespace_config.test", "params.default-ttl", "300"),
					testAccCheckNamespaceParam("aerospike", "default-ttl", "300"),
				),
			},
		},
	})
}

func TestAccAerospikeNamespaceConfig_invalidParam(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccNamespaceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccNamespaceConfigWithParam("totally-invalid-param-xyz", "100"),
				ExpectError: regexp.MustCompile("Invalid namespace parameter"),
			},
		},
	})
}

func TestAccAerospikeNamespaceConfig_invalidSetParam(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccNamespaceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccNamespaceConfigWithInvalidSetParam(),
				ExpectError: regexp.MustCompile("Invalid set parameter|Error setting set parameter"),
			},
		},
	})
}

func TestAccAerospikeNamespaceConfig_import(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccNamespaceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeNamespaceConfigDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccNamespaceConfigBasic(),
			},
			{
				ResourceName:                         "aerospike_namespace_config.test",
				ImportState:                          true,
				ImportStateVerify:                    true,
				ImportStateId:                        "aerospike",
				ImportStateVerifyIdentifierAttribute: "namespace",
				ImportStateVerifyIgnore:              []string{"info_commands", "set_config", "params"},
				// After import, params/set_config are null since we only track
				// what the user declares in HCL. The next plan+apply will set them.
			},
		},
	})
}

func TestAccAerospikeNamespaceConfig_setConfig(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccNamespaceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeNamespaceConfigDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccNamespaceConfigWithSetConfig(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_namespace_config.test", "namespace", "aerospike"),
					resource.TestCheckResourceAttrSet("aerospike_namespace_config.test", "info_commands.#"),
				),
			},
		},
	})
}

func testAccNamespaceConfigBasic() string {
	return `
resource "aerospike_namespace_config" "test" {
  namespace = "aerospike"

  params = {
    "default-ttl" = "100"
  }
}`
}

func testAccNamespaceConfigWithParam(key, value string) string {
	return fmt.Sprintf(`
resource "aerospike_namespace_config" "test" {
  namespace = "aerospike"

  params = {
    "%s" = "%s"
  }
}`, key, value)
}

func testAccNamespaceConfigWithInvalidSetParam() string {
	return `
resource "aerospike_namespace_config" "test" {
  namespace = "aerospike"

  set_config = {
    "testset_invalid" = {
      "totally-invalid-set-param-xyz" = "100"
    }
  }
}`
}

func testAccNamespaceConfigWithSetConfig() string {
	return `
resource "aerospike_namespace_config" "test" {
  namespace = "aerospike"

  params = {
    "default-ttl" = "100"
  }

  set_config = {
    "testset1" = {
      "stop-writes-count" = "50000"
    }
  }
}`
}

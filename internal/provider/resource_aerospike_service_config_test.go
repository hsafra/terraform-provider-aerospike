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

// testAccServiceConfigPreCheck ensures the admin user has sys-admin role
// required for set-config commands.
func testAccServiceConfigPreCheck(t *testing.T) {
	t.Helper()
	testAccPreCheck(t)

	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to connect to Aerospike: %s", err)
	}
	defer client.Close()

	adminPol := as.NewAdminPolicy()
	_ = client.GrantRoles(adminPol, "admin", []string{"sys-admin"})
	// Close and reconnect so the new roles take effect on the connection
	client.Close()

	client, err = testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to reconnect to Aerospike after granting roles: %s", err)
	}
	defer client.Close()
}

// testAccGetServiceParam reads a service config parameter directly from Aerospike.
func testAccGetServiceParam(key string) (string, error) {
	client, err := testAccGetAerospikeClient()
	if err != nil {
		return "", err
	}
	defer client.Close()

	config, err := getServiceConfig(client)
	if err != nil {
		return "", err
	}

	val, ok := config[key]
	if !ok {
		return "", fmt.Errorf("parameter %q not found in service config", key)
	}
	return val, nil
}

// testAccCheckServiceParam verifies a service parameter has the expected value on the server.
func testAccCheckServiceParam(key, expected string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		actual, err := testAccGetServiceParam(key)
		if err != nil {
			return err
		}
		if actual != expected {
			return fmt.Errorf("service param %q: expected %q, got %q", key, expected, actual)
		}
		return nil
	}
}

// testAccCheckAerospikeServiceConfigDestroy verifies the resource is removed from state.
// Service config always persists on the server, so we only check state removal.
func testAccCheckAerospikeServiceConfigDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aerospike_service_config" {
			continue
		}
		// Resource should be removed from state — nothing to check server-side
		// since service config always exists
	}
	return nil
}

func TestAccAerospikeServiceConfig_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeServiceConfigDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccServiceConfigBasic(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-max", "25000"),
					resource.TestCheckResourceAttrSet("aerospike_service_config.test", "info_commands.#"),
					testAccCheckServiceParam("proto-fd-max", "25000"),
				),
			},
		},
	})
}

func TestAccAerospikeServiceConfig_update(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeServiceConfigDestroy,
		Steps: []resource.TestStep{
			// Create with initial value
			{
				Config: testAccServiceConfigWithParam("proto-fd-max", "25000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-max", "25000"),
					testAccCheckServiceParam("proto-fd-max", "25000"),
				),
			},
			// Update the value
			{
				Config: testAccServiceConfigWithParam("proto-fd-max", "30000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-max", "30000"),
					testAccCheckServiceParam("proto-fd-max", "30000"),
				),
			},
		},
	})
}

func TestAccAerospikeServiceConfig_invalidParam(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccServiceConfigWithParam("totally-invalid-param-xyz", "100"),
				ExpectError: regexp.MustCompile("Invalid service parameter"),
			},
		},
	})
}

func TestAccAerospikeServiceConfig_import(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeServiceConfigDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccServiceConfigBasic(),
			},
			{
				ResourceName:      "aerospike_service_config.test",
				ImportState:       true,
				ImportStateVerify: false,
				ImportStateId:     "service",
				// After import, params are null since we only track
				// what the user declares in HCL. The next plan+apply will set them.
			},
		},
	})
}

func TestAccAerospikeServiceConfig_singleton(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccServiceConfigDuplicate(),
				ExpectError: regexp.MustCompile("Duplicate service config resource"),
			},
		},
	})
}

func TestAccAerospikeServiceConfig_multipleParams(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeServiceConfigDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccServiceConfigMultipleParams(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-max", "25000"),
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-idle-ms", "60001"),
					testAccCheckServiceParam("proto-fd-max", "25000"),
					testAccCheckServiceParam("proto-fd-idle-ms", "60001"),
				),
			},
		},
	})
}

// #19: Remove a param (go from 2 to 1).
func TestAccAerospikeServiceConfig_removeParam(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeServiceConfigDestroy,
		Steps: []resource.TestStep{
			// Create with two params
			{
				Config: testAccServiceConfigMultipleParams(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-max", "25000"),
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-idle-ms", "60001"),
				),
			},
			// Remove one param — should warn but succeed
			{
				Config: testAccServiceConfigWithParam("proto-fd-max", "25000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-max", "25000"),
					resource.TestCheckNoResourceAttr("aerospike_service_config.test", "params.proto-fd-idle-ms"),
				),
			},
		},
	})
}

// #20: Update multiple params in one step.
func TestAccAerospikeServiceConfig_updateMultipleParams(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeServiceConfigDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccServiceConfigMultipleParams(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-max", "25000"),
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-idle-ms", "60001"),
					testAccCheckServiceParam("proto-fd-max", "25000"),
					testAccCheckServiceParam("proto-fd-idle-ms", "60001"),
				),
			},
			// Update both params at once
			{
				Config: testAccServiceConfigMultipleParamsUpdated(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-max", "30000"),
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-idle-ms", "70000"),
					testAccCheckServiceParam("proto-fd-max", "30000"),
					testAccCheckServiceParam("proto-fd-idle-ms", "70000"),
				),
			},
		},
	})
}

// #21: Server drift detection.
func TestAccAerospikeServiceConfig_serverDrift(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeServiceConfigDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccServiceConfigWithParam("proto-fd-max", "25000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckServiceParam("proto-fd-max", "25000"),
				),
			},
			// Drift: modify param on server directly, then re-apply
			{
				PreConfig: func() {
					client, err := testAccGetAerospikeClient()
					if err != nil {
						t.Fatalf("failed to get client: %s", err)
					}
					defer client.Close()
					_, _ = setServiceParam(client, "proto-fd-max", "28000")
				},
				Config: testAccServiceConfigWithParam("proto-fd-max", "25000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "params.proto-fd-max", "25000"),
					testAccCheckServiceParam("proto-fd-max", "25000"),
				),
			},
		},
	})
}

// #22: Empty params map.
func TestAccAerospikeServiceConfig_emptyParams(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccServiceConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeServiceConfigDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccServiceConfigEmptyParams(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_service_config.test", "info_commands.#", "0"),
				),
			},
		},
	})
}

func testAccServiceConfigMultipleParamsUpdated() string {
	return `
resource "aerospike_service_config" "test" {
  params = {
    "proto-fd-max"     = "30000"
    "proto-fd-idle-ms" = "70000"
  }
}`
}

func testAccServiceConfigEmptyParams() string {
	return `
resource "aerospike_service_config" "test" {
  params = {}
}`
}

func testAccServiceConfigMultipleParams() string {
	return `
resource "aerospike_service_config" "test" {
  params = {
    "proto-fd-max"     = "25000"
    "proto-fd-idle-ms" = "60001"
  }
}`
}

func testAccServiceConfigBasic() string {
	return `
resource "aerospike_service_config" "test" {
  params = {
    "proto-fd-max" = "25000"
  }
}`
}

func testAccServiceConfigWithParam(key, value string) string {
	return fmt.Sprintf(`
resource "aerospike_service_config" "test" {
  params = {
    "%s" = "%s"
  }
}`, key, value)
}

func testAccServiceConfigDuplicate() string {
	return `
resource "aerospike_service_config" "first" {
  params = {
    "proto-fd-max" = "25000"
  }
}

resource "aerospike_service_config" "second" {
  params = {
    "proto-fd-max" = "30000"
  }
}`
}

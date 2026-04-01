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

// testAccXDRDCConfigPreCheck ensures the admin user has sys-admin role
// required for set-config commands.
func testAccXDRDCConfigPreCheck(t *testing.T) {
	t.Helper()
	testAccPreCheck(t)

	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to connect to Aerospike: %s", err)
	}
	defer client.Close()

	adminPol := as.NewAdminPolicy()
	_ = client.GrantRoles(adminPol, "admin", []string{"sys-admin"})
	client.Close()

	client, err = testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to reconnect to Aerospike after granting roles: %s", err)
	}
	defer client.Close()

	// Clean up any leftover test DCs from previous runs
	_ = cleanupTestDC(client, "test-dc")
	_ = cleanupTestDC(client, "test-dc-update")
}

// cleanupTestDC attempts to remove a DC — ignores errors (DC may not exist).
func cleanupTestDC(conn *as.Client, dc string) error {
	// Try removing namespace, nodes, then DC
	_, _ = removeXDRDCNamespace(conn, dc, "aerospike")
	_, _ = removeXDRDCNode(conn, dc, "aerospike-target:3000")
	return removeXDRDC(conn, dc)
}

// testAccCheckXDRDCExists verifies the DC exists on the Aerospike server.
func testAccCheckXDRDCExists(dc string) resource.TestCheckFunc { //nolint:unparam // dc will vary as more tests are added
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		if !dcExists(client, dc) {
			return fmt.Errorf("DC %q does not exist on the server", dc)
		}
		return nil
	}
}

// testAccCheckXDRDCDestroy verifies the DC has been removed from the server.
func testAccCheckXDRDCDestroy(s *terraform.State) error {
	client, err := testAccGetAerospikeClient()
	if err != nil {
		return err
	}
	defer client.Close()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aerospike_xdr_dc_config" {
			continue
		}
		dc := rs.Primary.Attributes["dc"]
		if dcExists(client, dc) {
			return fmt.Errorf("DC %q still exists after destroy", dc)
		}
	}
	return nil
}

func TestAccAerospikeXDRDCConfig_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigBasic(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckResourceAttrSet("aerospike_xdr_dc_config.test", "info_commands.#"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_withNamespace(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithNamespace(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.name", "aerospike"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_withSetPolicyShipSets(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithShipSets(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "true"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_withSetPolicyIgnoreSets(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithIgnoreSets(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "false"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_setPolicyConflict(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccXDRDCConfigSetPolicyConflict(),
				ExpectError: regexp.MustCompile("ship_sets and ignore_sets cannot both be specified"),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_setPolicyShipSetsRequiresFlag(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccXDRDCConfigShipSetsWithoutFlag(),
				ExpectError: regexp.MustCompile("ship_sets requires ship_only_specified_sets=true"),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_reservedParamInNamespace(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccXDRDCConfigReservedParam(),
				ExpectError: regexp.MustCompile("must be managed via the set_policy"),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_import(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigBasic(),
			},
			{
				ResourceName:      "aerospike_xdr_dc_config.test",
				ImportState:       true,
				ImportStateVerify: false,
				ImportStateId:     "test-dc",
			},
		},
	})
}

// --- Test config helpers ---

func testAccXDRDCConfigBasic() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"
}`
}

func testAccXDRDCConfigWithNamespace() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  namespace {
    name = "aerospike"
  }
}`
}

func testAccXDRDCConfigWithShipSets() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = true
      ship_sets = ["users", "orders"]
    }
  }
}`
}

func testAccXDRDCConfigWithIgnoreSets() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = false
      ignore_sets = ["temp", "cache"]
    }
  }
}`
}

func testAccXDRDCConfigSetPolicyConflict() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = true
      ship_sets   = ["users"]
      ignore_sets = ["temp"]
    }
  }
}`
}

func testAccXDRDCConfigShipSetsWithoutFlag() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = false
      ship_sets = ["users"]
    }
  }
}`
}

func testAccXDRDCConfigReservedParam() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  namespace {
    name = "aerospike"

    params = {
      "ship-only-specified-sets" = "true"
    }
  }
}`
}

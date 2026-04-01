// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"regexp"
	"strings"
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
	// Try removing namespaces, nodes, then DC
	_, _ = removeXDRDCNamespace(conn, dc, "aerospike")
	_, _ = removeXDRDCNamespace(conn, dc, "aerospike2")
	_, _ = removeXDRDCNode(conn, dc, "aerospike-target:3000")
	_, _ = removeXDRDCNode(conn, dc, "aerospike-target2:3000")
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
					testAccCheckXDRDCNamespaceParam("ship-only-specified-sets", "true"),
					testAccCheckXDRDCNamespaceShipSets([]string{"users", "orders"}),
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
					testAccCheckXDRDCNamespaceParam("ship-only-specified-sets", "false"),
					testAccCheckXDRDCNamespaceIgnoreSets("test-dc", "aerospike", []string{"temp", "cache"}),
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

func TestAccAerospikeXDRDCConfig_updateAddNamespace(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create DC without namespace
			{
				Config: testAccXDRDCConfigBasic(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
			// Update: add a namespace
			{
				Config: testAccXDRDCConfigWithNamespace(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.name", "aerospike"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_updateRemoveNamespace(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create DC with namespace
			{
				Config: testAccXDRDCConfigWithNamespace(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.name", "aerospike"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
			// Update: remove the namespace
			{
				Config: testAccXDRDCConfigBasic(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckNoResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.name"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_updateNodes(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create DC with one node
			{
				Config: testAccXDRDCConfigWithNodes([]string{"aerospike-target:3000"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.#", "1"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.0", "aerospike-target:3000"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
			// Update: remove node (empty list)
			{
				Config: testAccXDRDCConfigBasic(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_withDCParams(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create DC with params (period-ms range: 5–1000)
			{
				Config: testAccXDRDCConfigWithDCParams("200"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "params.period-ms", "200"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCParam("test-dc", "period-ms", "200"),
				),
			},
			// Update DC params
			{
				Config: testAccXDRDCConfigWithDCParams("500"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "params.period-ms", "500"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCParam("test-dc", "period-ms", "500"),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_withNamespaceParams(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create DC with namespace params (max-throughput: 0 = unlimited)
			{
				Config: testAccXDRDCConfigWithNamespaceParams("100000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.name", "aerospike"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.params.max-throughput", "100000"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceParam("max-throughput", "100000"),
				),
			},
			// Update namespace params
			{
				Config: testAccXDRDCConfigWithNamespaceParams("200000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.params.max-throughput", "200000"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceParam("max-throughput", "200000"),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_updateSetPolicy(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create with ship_sets policy
			{
				Config: testAccXDRDCConfigWithShipSets(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "true"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceParam("ship-only-specified-sets", "true"),
					testAccCheckXDRDCNamespaceShipSets([]string{"users", "orders"}),
				),
			},
			// Update: switch to ignore_sets policy
			{
				Config: testAccXDRDCConfigWithIgnoreSets(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "false"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceParam("ship-only-specified-sets", "false"),
					testAccCheckXDRDCNamespaceShipSets([]string{}),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_updateShipSets(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create with ship_sets = ["users", "orders"]
			{
				Config: testAccXDRDCConfigWithShipSets(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "true"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceShipSets([]string{"users", "orders"}),
				),
			},
			// Update: change ship_sets to different sets
			{
				Config: testAccXDRDCConfigWithShipSetsUpdated(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "true"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceShipSets([]string{"products", "accounts"}),
				),
			},
		},
	})
}

func TestAccAerospikeXDRDCConfig_updateIgnoreSets(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create with ignore_sets = ["temp"]
			{
				Config: testAccXDRDCConfigWithIgnoreSetsOne(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "false"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ignore_sets.#", "1"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceIgnoreSets("test-dc", "aerospike", []string{"temp"}),
				),
			},
			// Update: add another ignore-set → ["temp", "cache"]
			{
				Config: testAccXDRDCConfigWithIgnoreSets(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "false"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ignore_sets.#", "2"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceIgnoreSets("test-dc", "aerospike", []string{"temp", "cache"}),
				),
			},
		},
	})
}

// #23: DC with multiple namespaces.
func TestAccAerospikeXDRDCConfig_multipleNamespaces(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithMultipleNamespaces(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.#", "2"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

// #24: Namespace rewind attribute.
func TestAccAerospikeXDRDCConfig_namespaceRewind(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithRewind("all"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.name", "aerospike"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.rewind", "all"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

// #24b: Namespace rewind with numeric seconds.
func TestAccAerospikeXDRDCConfig_namespaceRewindSeconds(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithRewind("100"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "dc", "test-dc"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.rewind", "100"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

// #25: Remove set_policy block.
func TestAccAerospikeXDRDCConfig_removeSetPolicy(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create with ship_sets policy
			{
				Config: testAccXDRDCConfigWithShipSets(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "true"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceShipSets([]string{"users", "orders"}),
				),
			},
			// Remove set_policy entirely — keep namespace but no set_policy block
			{
				Config: testAccXDRDCConfigWithNamespace(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.name", "aerospike"),
					resource.TestCheckNoResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceParam("ship-only-specified-sets", "false"),
				),
			},
		},
	})
}

// #26: ignore_sets with ship_only_specified_sets=true should error.
func TestAccAerospikeXDRDCConfig_ignoreSetsWithShipOnlyTrue(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccXDRDCConfigIgnoreSetsWithShipOnlyTrue(),
				ExpectError: regexp.MustCompile("ignore_sets requires ship_only_specified_sets=false"),
			},
		},
	})
}

// #27: Reserved param in DC-level params.
func TestAccAerospikeXDRDCConfig_reservedParamInDCParams(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccXDRDCConfigReservedDCParam(),
				ExpectError: regexp.MustCompile("must be managed via the set_policy"),
			},
		},
	})
}

// #28: Multiple nodes — add, swap, and diff.
func TestAccAerospikeXDRDCConfig_updateMultipleNodes(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create DC with one node
			{
				Config: testAccXDRDCConfigWithNodes([]string{"aerospike-target:3000"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.#", "1"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
			// Update to two nodes (add second)
			{
				Config: testAccXDRDCConfigWithNodes([]string{"aerospike-target:3000", "aerospike-target2:3000"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.#", "2"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
			// Remove one node, keep the other
			{
				Config: testAccXDRDCConfigWithNodes([]string{"aerospike-target2:3000"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.#", "1"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.0", "aerospike-target2:3000"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

// #29: DC params and namespace params together.
func TestAccAerospikeXDRDCConfig_dcParamsAndNamespaceParamsTogether(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithDCAndNamespaceParams("200", "100000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "params.period-ms", "200"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.params.max-throughput", "100000"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCParam("test-dc", "period-ms", "200"),
					testAccCheckXDRDCNamespaceParam("max-throughput", "100000"),
				),
			},
		},
	})
}

// #30: DC disappears outside Terraform.
func TestAccAerospikeXDRDCConfig_disappearsOutsideTerraform(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigBasic(),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckXDRDCExists("test-dc"),
				),
			},
			// Delete DC outside Terraform, then re-apply — should recreate
			{
				PreConfig: func() {
					client, err := testAccGetAerospikeClient()
					if err != nil {
						t.Fatalf("failed to get client: %s", err)
					}
					defer client.Close()
					_ = removeXDRDC(client, "test-dc")
				},
				Config: testAccXDRDCConfigBasic(),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckXDRDCExists("test-dc"),
				),
			},
		},
	})
}

// #31: Import a DC that has namespaces configured.
func TestAccAerospikeXDRDCConfig_importWithNamespace(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithNamespace(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.name", "aerospike"),
					testAccCheckXDRDCExists("test-dc"),
				),
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

// testAccCheckXDRDCNamespaceIgnoreSets verifies the server has exactly the expected ignore-sets.
func testAccCheckXDRDCNamespaceIgnoreSets(dc, namespace string, expected []string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		config, err := getXDRDCNamespaceConfig(client, dc, namespace)
		if err != nil {
			return fmt.Errorf("failed to get XDR DC namespace config: %s", err)
		}

		return compareStringSet(config["ignored-sets"], expected, "ignore-sets")
	}
}

// testAccCheckXDRDCNamespaceShipSets verifies the server has exactly the expected ship-sets.
func testAccCheckXDRDCNamespaceShipSets(expected []string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		config, err := getXDRDCNamespaceConfig(client, "test-dc", "aerospike")
		if err != nil {
			return fmt.Errorf("failed to get XDR DC namespace config: %s", err)
		}

		actual := config["shipped-sets"]
		return compareStringSet(actual, expected, "ship-sets")
	}
}

// testAccCheckXDRDCParam verifies a DC-level config parameter on the server.
func testAccCheckXDRDCParam(dc, key, expected string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		config, err := getXDRDCConfig(client, dc)
		if err != nil {
			return fmt.Errorf("failed to get XDR DC config: %s", err)
		}

		actual, ok := config[key]
		if !ok {
			return fmt.Errorf("DC param %q not found in server config", key)
		}
		if actual != expected {
			return fmt.Errorf("DC param %q: expected %q, got %q", key, expected, actual)
		}
		return nil
	}
}

// testAccCheckXDRDCNamespaceParam verifies a namespace-level XDR config parameter on the server.
func testAccCheckXDRDCNamespaceParam(key, expected string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		config, err := getXDRDCNamespaceConfig(client, "test-dc", "aerospike")
		if err != nil {
			return fmt.Errorf("failed to get XDR DC namespace config: %s", err)
		}

		actual, ok := config[key]
		if !ok {
			return fmt.Errorf("namespace param %q not found in server config for %s/%s", key, "test-dc", "aerospike")
		}
		if actual != expected {
			return fmt.Errorf("namespace param %q: expected %q, got %q", key, expected, actual)
		}
		return nil
	}
}

// compareStringSet compares a comma-separated server value against an expected set of strings.
func compareStringSet(actual string, expected []string, label string) error {
	expectedSet := make(map[string]bool)
	for _, e := range expected {
		expectedSet[e] = true
	}

	actualSet := make(map[string]bool)
	if actual != "" {
		for _, s := range strings.Split(actual, ",") {
			actualSet[strings.TrimSpace(s)] = true
		}
	}

	if len(actualSet) != len(expectedSet) {
		return fmt.Errorf("expected %s %v but got %q", label, expected, actual)
	}
	for e := range expectedSet {
		if !actualSet[e] {
			return fmt.Errorf("expected %s entry %q not found in server value %q", label, e, actual)
		}
	}
	return nil
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

func testAccXDRDCConfigWithIgnoreSetsOne() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = false
      ignore_sets = ["temp"]
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

func testAccXDRDCConfigWithNodes(nodes []string) string {
	nodeList := ""
	for i, n := range nodes {
		if i > 0 {
			nodeList += ", "
		}
		nodeList += fmt.Sprintf("%q", n)
	}
	return fmt.Sprintf(`
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = [%s]
}`, nodeList)
}

func testAccXDRDCConfigWithDCParams(periodMs string) string {
	return fmt.Sprintf(`
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  params = {
    "period-ms" = "%s"
  }
}`, periodMs)
}

func testAccXDRDCConfigWithNamespaceParams(maxThroughput string) string {
	return fmt.Sprintf(`
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  namespace {
    name = "aerospike"

    params = {
      "max-throughput" = "%s"
    }
  }
}`, maxThroughput)
}

func testAccXDRDCConfigWithShipSetsUpdated() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = true
      ship_sets = ["products", "accounts"]
    }
  }
}`
}

func testAccXDRDCConfigWithMultipleNamespaces() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  namespace {
    name = "aerospike"
  }

  namespace {
    name = "aerospike2"
  }
}`
}

func testAccXDRDCConfigWithRewind(rewind string) string {
	return fmt.Sprintf(`
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  namespace {
    name   = "aerospike"
    rewind = "%s"
  }
}`, rewind)
}

func testAccXDRDCConfigIgnoreSetsWithShipOnlyTrue() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = true
      ignore_sets = ["temp"]
    }
  }
}`
}

func testAccXDRDCConfigReservedDCParam() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  params = {
    "ship-set" = "users"
  }
}`
}

func testAccXDRDCConfigWithDCAndNamespaceParams(periodMs, maxThroughput string) string {
	return fmt.Sprintf(`
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target:3000"]

  params = {
    "period-ms" = "%s"
  }

  namespace {
    name = "aerospike"

    params = {
      "max-throughput" = "%s"
    }
  }
}`, periodMs, maxThroughput)
}

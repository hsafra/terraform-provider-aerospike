// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/types"
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
	_, _ = removeXDRDCNode(conn, dc, "aerospike-target-1:3000")
	_, _ = removeXDRDCNode(conn, dc, "aerospike-target-2:3000")
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
				Config: testAccXDRDCConfigWithNodes([]string{"aerospike-target-1:3000"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.#", "1"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.0", "aerospike-target-1:3000"),
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

// Regression: removing a set from ignore_sets should be detected by plan and applied.
func TestAccAerospikeXDRDCConfig_removeIgnoreSet(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Step 1: Create with ignore_sets = ["set1"]
			{
				Config: testAccXDRDCConfigIgnoreSetsN([]string{"set1"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ignore_sets.#", "1"),
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCNamespaceIgnoreSets("test-dc", "aerospike", []string{"set1"}),
				),
			},
			// Step 2: Add set2 → ignore_sets = ["set1", "set2"]
			{
				Config: testAccXDRDCConfigIgnoreSetsN([]string{"set1", "set2"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ignore_sets.#", "2"),
					testAccCheckXDRDCNamespaceIgnoreSets("test-dc", "aerospike", []string{"set1", "set2"}),
				),
			},
			// Step 3: Remove set2 → ignore_sets = ["set1"]
			// This must detect the change and remove set2 from ignored-sets on the server.
			{
				Config: testAccXDRDCConfigIgnoreSetsN([]string{"set1"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ignore_sets.#", "1"),
					testAccCheckXDRDCNamespaceIgnoreSets("test-dc", "aerospike", []string{"set1"}),
				),
			},
		},
	})
}

// Regression: removing ALL sets from ignore_sets (going to empty) should be detected.
func TestAccAerospikeXDRDCConfig_removeAllIgnoreSets(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Step 1: Create with ignore_sets = ["set1", "set2"]
			{
				Config: testAccXDRDCConfigIgnoreSetsN([]string{"set1", "set2"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ignore_sets.#", "2"),
					testAccCheckXDRDCNamespaceIgnoreSets("test-dc", "aerospike", []string{"set1", "set2"}),
				),
			},
			// Step 2: Remove ignore_sets entirely, keep set_policy with just the flag
			{
				Config: testAccXDRDCConfigSetPolicyNoSets(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_only_specified_sets", "false"),
					testAccCheckXDRDCNamespaceIgnoreSets("test-dc", "aerospike", []string{}),
				),
			},
		},
	})
}

// Regression: removing a set from ship_sets should be detected and applied.
func TestAccAerospikeXDRDCConfig_removeShipSet(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Step 1: Create with ship_sets = ["set1"]
			{
				Config: testAccXDRDCConfigShipSetsN([]string{"set1"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_sets.#", "1"),
					testAccCheckXDRDCNamespaceShipSets([]string{"set1"}),
				),
			},
			// Step 2: Add set2 → ship_sets = ["set1", "set2"]
			{
				Config: testAccXDRDCConfigShipSetsN([]string{"set1", "set2"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_sets.#", "2"),
					testAccCheckXDRDCNamespaceShipSets([]string{"set1", "set2"}),
				),
			},
			// Step 3: Remove set2 → ship_sets = ["set1"]
			{
				Config: testAccXDRDCConfigShipSetsN([]string{"set1"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "namespace.0.set_policy.0.ship_sets.#", "1"),
					testAccCheckXDRDCNamespaceShipSets([]string{"set1"}),
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
				Config: testAccXDRDCConfigWithNodes([]string{"aerospike-target-1:3000"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.#", "1"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
			// Update to two nodes (add second)
			{
				Config: testAccXDRDCConfigWithNodes([]string{"aerospike-target-1:3000", "aerospike-target-2:3000"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.#", "2"),
					testAccCheckXDRDCExists("test-dc"),
				),
			},
			// Remove one node, keep the other
			{
				Config: testAccXDRDCConfigWithNodes([]string{"aerospike-target-2:3000"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.#", "1"),
					resource.TestCheckResourceAttr("aerospike_xdr_dc_config.test", "node_address_ports.0", "aerospike-target-2:3000"),
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

// #32: Multi-node cluster verification — config commands reach all nodes.
func TestAccAerospikeXDRDCConfig_multiNodeIgnoreSets(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccXDRDCConfigPreCheck(t)
			testAccRequireMultiNode(t)
		},
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			// Create with ignore_sets and verify ALL nodes have the same config
			{
				Config: testAccXDRDCConfigIgnoreSetsN([]string{"set1", "set2"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckAllNodesXDRNamespaceParam("test-dc", "aerospike", "ignored-sets", []string{"set1", "set2"}),
				),
			},
			// Remove a set and verify ALL nodes reflect the change
			{
				Config: testAccXDRDCConfigIgnoreSetsN([]string{"set1"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckAllNodesXDRNamespaceParam("test-dc", "aerospike", "ignored-sets", []string{"set1"}),
				),
			},
		},
	})
}

// #33: Multi-node cluster verification — service config reaches all nodes.
func TestAccAerospikeXDRDCConfig_multiNodeServiceConfig(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testAccRequireMultiNode(t)
		},
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccServiceConfigWithParam("proto-fd-max", "20000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckAllNodesServiceParam("proto-fd-max", "20000"),
				),
			},
		},
	})
}

// testAccRequireMultiNode skips the test if the cluster has fewer than 2 nodes.
func testAccRequireMultiNode(t *testing.T) {
	t.Helper()
	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to connect to Aerospike: %s", err)
	}
	defer client.Close()

	nodes := client.GetNodes()
	if len(nodes) < 2 {
		t.Skipf("Skipping multi-node test: cluster has %d node(s), need at least 2", len(nodes))
	}
}

// testAccCheckAllNodesXDRNamespaceParam verifies that a comma-separated XDR namespace config
// parameter has the expected values on ALL cluster nodes (not just the one terraform connects to).
func testAccCheckAllNodesXDRNamespaceParam(dc, namespace, key string, expected []string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		command := "get-config:context=xdr;dc=" + dc + ";namespace=" + namespace
		nodes := client.GetNodes()
		for _, node := range nodes {
			result, err := sendInfoToNode(node, command)
			if err != nil {
				return fmt.Errorf("node %s: failed to get XDR namespace config: %w", node.GetName(), err)
			}

			config := make(map[string]string)
			raw := result[command]
			pairs := strings.Split(raw, ";")
			for _, pair := range pairs {
				kv := strings.SplitN(pair, "=", 2)
				if len(kv) == 2 {
					config[kv[0]] = kv[1]
				}
			}

			if err := compareStringSet(config[key], expected, key); err != nil {
				return fmt.Errorf("node %s: %w", node.GetName(), err)
			}
		}
		return nil
	}
}

// testAccCheckAllNodesServiceParam verifies a service config parameter on ALL cluster nodes.
func testAccCheckAllNodesServiceParam(key, expected string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		command := "get-config:context=service"
		nodes := client.GetNodes()
		for _, node := range nodes {
			result, err := sendInfoToNode(node, command)
			if err != nil {
				return fmt.Errorf("node %s: failed to get service config: %w", node.GetName(), err)
			}

			raw := result[command]
			pairs := strings.Split(raw, ";")
			config := make(map[string]string)
			for _, pair := range pairs {
				kv := strings.SplitN(pair, "=", 2)
				if len(kv) == 2 {
					config[kv[0]] = kv[1]
				}
			}

			actual, ok := config[key]
			if !ok {
				return fmt.Errorf("node %s: service param %q not found", node.GetName(), key)
			}
			if actual != expected {
				return fmt.Errorf("node %s: service param %q: expected %q, got %q", node.GetName(), key, expected, actual)
			}
		}
		return nil
	}
}

// testAccCheckXDRDCNamespaceIgnoreSets verifies the server has exactly the expected ignore-sets.
func testAccCheckXDRDCNamespaceIgnoreSets(dc, namespace string, expected []string) resource.TestCheckFunc { //nolint:unparam // dc will vary as more tests are added
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		config, err := getXDRDCNamespaceConfig(client, dc, namespace)
		if err != nil {
			return fmt.Errorf("failed to get XDR DC namespace config: %w", err)
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
			return fmt.Errorf("failed to get XDR DC namespace config: %w", err)
		}

		actual := config["shipped-sets"]
		return compareStringSet(actual, expected, "ship-sets")
	}
}

// testAccCheckXDRDCParam verifies a DC-level config parameter on the server.
func testAccCheckXDRDCParam(dc, key, expected string) resource.TestCheckFunc { //nolint:unparam // dc will vary as more tests are added
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		config, err := getXDRDCConfig(client, dc)
		if err != nil {
			return fmt.Errorf("failed to get XDR DC config: %w", err)
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
			return fmt.Errorf("failed to get XDR DC namespace config: %w", err)
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

  node_address_ports = ["aerospike-target-1:3000"]

  namespace {
    name = "aerospike"
  }
}`
}

func testAccXDRDCConfigWithShipSets() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target-1:3000"]

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

  node_address_ports = ["aerospike-target-1:3000"]

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

  node_address_ports = ["aerospike-target-1:3000"]

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

  node_address_ports = ["aerospike-target-1:3000"]

  params = {
    "period-ms" = "%s"
  }
}`, periodMs)
}

func testAccXDRDCConfigWithNamespaceParams(maxThroughput string) string {
	return fmt.Sprintf(`
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target-1:3000"]

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

  node_address_ports = ["aerospike-target-1:3000"]

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

  node_address_ports = ["aerospike-target-1:3000"]

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

  node_address_ports = ["aerospike-target-1:3000"]

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

func testAccXDRDCConfigSetPolicyNoSets() string {
	return `
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target-1:3000"]

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = false
    }
  }
}`
}

func testAccXDRDCConfigShipSetsN(sets []string) string {
	quoted := make([]string, len(sets))
	for i, s := range sets {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf(`
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target-1:3000"]

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = true
      ship_sets = [%s]
    }
  }
}`, strings.Join(quoted, ", "))
}

func testAccXDRDCConfigIgnoreSetsN(sets []string) string {
	quoted := make([]string, len(sets))
	for i, s := range sets {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf(`
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target-1:3000"]

  namespace {
    name = "aerospike"

    set_policy {
      ship_only_specified_sets = false
      ignore_sets = [%s]
    }
  }
}`, strings.Join(quoted, ", "))
}

func testAccXDRDCConfigWithDCAndNamespaceParams(periodMs, maxThroughput string) string {
	return fmt.Sprintf(`
resource "aerospike_xdr_dc_config" "test" {
  dc = "test-dc"

  node_address_ports = ["aerospike-target-1:3000"]

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

// --- Unit tests for pure helpers ---

func TestSplitCommaList(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{"a,b", []string{"a", "b"}},
		{"a,,b,", []string{"a", "b"}},
		{" a , b ", []string{"a", "b"}},
	}
	for _, tt := range tests {
		if got := splitCommaList(tt.in); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("splitCommaList(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestExtractStringListAndSlice(t *testing.T) {
	list := types.ListValueMust(types.StringType, []attr.Value{types.StringValue("b"), types.StringValue("a")})
	if got := extractStringList(list); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Errorf("extractStringList = %v, want list order kept", got)
	}
	if got := extractStringList(types.ListNull(types.StringType)); got != nil {
		t.Errorf("null list: got %v", got)
	}
	if got := extractStringList(types.ListUnknown(types.StringType)); got != nil {
		t.Errorf("unknown list: got %v", got)
	}

	set := types.SetValueMust(types.StringType, []attr.Value{types.StringValue("x"), types.StringValue("y")})
	got := extractStringSlice(set)
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Errorf("extractStringSlice = %v", got)
	}
	if got := extractStringSlice(types.SetNull(types.StringType)); got != nil {
		t.Errorf("null set: got %v", got)
	}
	if got := extractStringSlice(types.SetUnknown(types.StringType)); got != nil {
		t.Errorf("unknown set: got %v", got)
	}
}

func TestToStringSet(t *testing.T) {
	got := toStringSet([]string{"a", "b", "a"})
	if !reflect.DeepEqual(got, map[string]bool{"a": true, "b": true}) {
		t.Errorf("got %v", got)
	}
	if len(toStringSet(nil)) != 0 {
		t.Error("nil input should give an empty set")
	}
}

func TestExtractOldSets(t *testing.T) {
	shipSets := func(p XDRSetPolicyModel) types.Set { return p.ShipSets }

	if got := extractOldSets(nil, shipSets); len(got) != 0 {
		t.Errorf("no policy: got %v", got)
	}
	nullPolicy := []XDRSetPolicyModel{{ShipSets: types.SetNull(types.StringType)}}
	if got := extractOldSets(nullPolicy, shipSets); len(got) != 0 {
		t.Errorf("null set: got %v", got)
	}
	unknownPolicy := []XDRSetPolicyModel{{ShipSets: types.SetUnknown(types.StringType)}}
	if got := extractOldSets(unknownPolicy, shipSets); len(got) != 0 {
		t.Errorf("unknown set: got %v", got)
	}
	policy := []XDRSetPolicyModel{{ShipSets: types.SetValueMust(types.StringType,
		[]attr.Value{types.StringValue("s1"), types.StringValue("s2")})}}
	if got := extractOldSets(policy, shipSets); !reflect.DeepEqual(got, map[string]bool{"s1": true, "s2": true}) {
		t.Errorf("got %v", got)
	}
}

func TestRefreshXDRMembership(t *testing.T) {
	nodes := func(addrs ...string) types.List {
		vals := make([]attr.Value, 0, len(addrs))
		for _, a := range addrs {
			vals = append(vals, types.StringValue(a))
		}
		return types.ListValueMust(types.StringType, vals)
	}
	namespaces := func(names ...string) []XDRNamespaceModel {
		out := make([]XDRNamespaceModel, 0, len(names))
		for _, n := range names {
			out = append(out, XDRNamespaceModel{Name: types.StringValue(n)})
		}
		return out
	}
	nsNames := func(nss []XDRNamespaceModel) []string {
		out := make([]string, 0, len(nss))
		for _, ns := range nss {
			out = append(out, ns.Name.ValueString())
		}
		return out
	}

	t.Run("all present keeps state", func(t *testing.T) {
		gotNodes, gotNs := refreshXDRMembership(
			map[string]string{"node-address-port": "h1:3000,h2:3000", "namespaces": "ns1,ns2"},
			nodes("h2:3000", "h1:3000"), namespaces("ns2", "ns1"))
		if got := extractStringList(gotNodes); !reflect.DeepEqual(got, []string{"h2:3000", "h1:3000"}) {
			t.Errorf("nodes = %v, want state order kept", got)
		}
		if got := nsNames(gotNs); !reflect.DeepEqual(got, []string{"ns2", "ns1"}) {
			t.Errorf("namespaces = %v, want state order kept", got)
		}
	})

	t.Run("detached members are dropped", func(t *testing.T) {
		gotNodes, gotNs := refreshXDRMembership(
			map[string]string{"node-address-port": "h1:3000", "namespaces": "ns2"},
			nodes("h1:3000", "h2:3000"), namespaces("ns1", "ns2"))
		if got := extractStringList(gotNodes); !reflect.DeepEqual(got, []string{"h1:3000"}) {
			t.Errorf("nodes = %v", got)
		}
		if got := nsNames(gotNs); !reflect.DeepEqual(got, []string{"ns2"}) {
			t.Errorf("namespaces = %v", got)
		}
	})

	t.Run("empty server values drop everything", func(t *testing.T) {
		gotNodes, gotNs := refreshXDRMembership(
			map[string]string{"node-address-port": "", "namespaces": ""},
			nodes("h1:3000"), namespaces("ns1"))
		if gotNodes.IsNull() || len(gotNodes.Elements()) != 0 {
			t.Errorf("nodes = %v, want empty non-null list", gotNodes)
		}
		if gotNs == nil || len(gotNs) != 0 {
			t.Errorf("namespaces = %v, want empty non-nil slice", gotNs)
		}
	})

	t.Run("keys not reported leave state untouched", func(t *testing.T) {
		gotNodes, gotNs := refreshXDRMembership(map[string]string{}, nodes("h1:3000"), namespaces("ns1"))
		if got := extractStringList(gotNodes); !reflect.DeepEqual(got, []string{"h1:3000"}) {
			t.Errorf("nodes = %v", got)
		}
		if got := nsNames(gotNs); !reflect.DeepEqual(got, []string{"ns1"}) {
			t.Errorf("namespaces = %v", got)
		}
	})

	t.Run("unmanaged attributes stay null", func(t *testing.T) {
		gotNodes, gotNs := refreshXDRMembership(
			map[string]string{"node-address-port": "h1:3000", "namespaces": "ns1"},
			types.ListNull(types.StringType), nil)
		if !gotNodes.IsNull() {
			t.Errorf("nodes = %v, want null", gotNodes)
		}
		if gotNs != nil {
			t.Errorf("namespaces = %v, want nil", gotNs)
		}
	})
}

func TestXDRValidateConfig(t *testing.T) {
	r := &AerospikeXDRDCConfig{}
	set := func(names ...string) types.Set {
		vals := make([]attr.Value, 0, len(names))
		for _, n := range names {
			vals = append(vals, types.StringValue(n))
		}
		return types.SetValueMust(types.StringType, vals)
	}
	model := func(dcParams map[string]string, ns ...XDRNamespaceModel) AerospikeXDRDCConfigModel {
		params := types.MapNull(types.StringType)
		if dcParams != nil {
			params = stringMap(dcParams)
		}
		return AerospikeXDRDCConfigModel{DC: types.StringValue("dc"), Params: params, Namespaces: ns}
	}
	nsWithPolicy := func(shipOnly bool, ship, ignore types.Set) XDRNamespaceModel {
		return XDRNamespaceModel{
			Name:   types.StringValue("aerospike"),
			Params: types.MapNull(types.StringType),
			SetPolicy: []XDRSetPolicyModel{{
				ShipOnlySpecifiedSets: types.BoolValue(shipOnly),
				ShipSets:              ship,
				IgnoreSets:            ignore,
			}},
		}
	}
	null := types.SetNull(types.StringType)

	tests := []struct {
		name    string
		data    AerospikeXDRDCConfigModel
		wantErr string
	}{
		{"empty config", model(nil), ""},
		{"plain DC params", model(map[string]string{"period-ms": "100"}), ""},
		{"reserved key in DC params", model(map[string]string{"ship-set": "s1"}), "Reserved parameter in DC params"},
		{"reserved key in namespace params", model(nil, XDRNamespaceModel{
			Name: types.StringValue("aerospike"), Params: stringMap(map[string]string{"ignore-set": "s1"}),
		}), "Reserved parameter in namespace params"},
		{"ship sets with ship-only", model(nil, nsWithPolicy(true, set("s1"), null)), ""},
		{"ignore sets without ship-only", model(nil, nsWithPolicy(false, null, set("s1"))), ""},
		{"ship-only with no sets", model(nil, nsWithPolicy(true, null, null)), ""},
		{"empty sets are not 'specified'", model(nil, nsWithPolicy(false, set(), set())), ""},
		{"both lists", model(nil, nsWithPolicy(true, set("s1"), set("s2"))), "cannot both be specified"},
		{"ship sets without ship-only", model(nil, nsWithPolicy(false, set("s1"), null)), "ship_sets requires"},
		{"ignore sets with ship-only", model(nil, nsWithPolicy(true, null, set("s1"))), "ignore_sets requires"},
		{"unknown sets are skipped", model(nil, nsWithPolicy(false, types.SetUnknown(types.StringType), null)), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := r.validateConfig(context.Background(), tt.data)
			if tt.wantErr == "" {
				if diags.HasError() {
					t.Fatalf("unexpected errors: %v", diags)
				}
				return
			}
			if !diags.HasError() {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			found := false
			for _, d := range diags.Errors() {
				if strings.Contains(d.Summary()+" "+d.Detail(), tt.wantErr) {
					found = true
				}
			}
			if !found {
				t.Errorf("no error contains %q: %v", tt.wantErr, diags)
			}
		})
	}
}

// --- Acceptance tests ---

// testAccCheckXDRDCAbsent fails the test if dc exists on the server.
func testAccCheckXDRDCAbsent(t *testing.T, dc string) {
	t.Helper()
	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("failed to get client: %s", err)
	}
	defer client.Close()
	if dcExists(client, dc) {
		t.Fatalf("DC %q still exists after a failed create; it should have been rolled back", dc)
	}
}

// testAccCheckXDRDCMembers verifies the server's namespace and node lists for dc.
func testAccCheckXDRDCMembers(dc string, wantNamespaces, wantNodes []string) resource.TestCheckFunc { //nolint:unparam // dc will vary as more tests are added
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()
		cfg, err := getXDRDCConfig(client, dc)
		if err != nil {
			return err
		}
		gotNs := splitCommaList(cfg["namespaces"])
		gotNodes := splitCommaList(cfg["node-address-port"])
		sort.Strings(gotNs)
		sort.Strings(gotNodes)
		if !reflect.DeepEqual(gotNs, wantNamespaces) {
			return fmt.Errorf("DC %q namespaces = %v, want %v", dc, gotNs, wantNamespaces)
		}
		if !reflect.DeepEqual(gotNodes, wantNodes) {
			return fmt.Errorf("DC %q nodes = %v, want %v", dc, gotNodes, wantNodes)
		}
		return nil
	}
}

// A create that fails after the DC exists (invalid DC param) removes the DC
// again, so the next apply can create it instead of failing on "already exists".
func TestAccAerospikeXDRDCConfig_invalidDCParamRollsBack(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: `
resource "aerospike_xdr_dc_config" "test" {
  dc                 = "test-dc"
  node_address_ports = ["aerospike-target-1:3000"]
  params = {
    "bogus-param" = "1"
  }
}`,
				ExpectError: regexp.MustCompile("Error setting XDR DC parameter"),
			},
			{
				PreConfig: func() { testAccCheckXDRDCAbsent(t, "test-dc") },
				Config:    testAccXDRDCConfigWithNamespace(),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckXDRDCExists("test-dc"),
					testAccCheckXDRDCMembers("test-dc", []string{"aerospike"}, []string{"aerospike-target-1:3000"}),
				),
			},
		},
	})
}

// Same rollback when the failure comes from a namespace-level param, after
// nodes and the namespace have already been attached.
func TestAccAerospikeXDRDCConfig_invalidNamespaceParamRollsBack(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: `
resource "aerospike_xdr_dc_config" "test" {
  dc                 = "test-dc"
  node_address_ports = ["aerospike-target-1:3000"]

  namespace {
    name = "aerospike"
    params = {
      "bogus-param" = "1"
    }
  }
}`,
				ExpectError: regexp.MustCompile("Error setting XDR DC namespace parameter"),
			},
			{
				PreConfig: func() { testAccCheckXDRDCAbsent(t, "test-dc") },
				Config:    testAccXDRDCConfigWithNamespace(),
				Check:     testAccCheckXDRDCExists("test-dc"),
			},
		},
	})
}

// Importing a DC that does not exist fails with a clear error.
func TestAccAerospikeXDRDCConfig_importNonexistent(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:        testAccXDRDCConfigBasic(),
				ResourceName:  "aerospike_xdr_dc_config.test",
				ImportState:   true,
				ImportStateId: "no-such-dc",
				ExpectError:   regexp.MustCompile("DC not found"),
			},
		},
	})
}

// A namespace detached from the DC outside Terraform shows up in the plan and
// is re-added on apply.
func TestAccAerospikeXDRDCConfig_namespaceRemovedOutsideTerraform(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithNamespace(),
				Check:  testAccCheckXDRDCMembers("test-dc", []string{"aerospike"}, []string{"aerospike-target-1:3000"}),
			},
			{
				PreConfig: func() {
					client, err := testAccGetAerospikeClient()
					if err != nil {
						t.Fatalf("failed to get client: %s", err)
					}
					defer client.Close()
					if _, err := removeXDRDCNamespace(client, "test-dc", "aerospike"); err != nil {
						t.Fatalf("failed to remove namespace outside Terraform: %s", err)
					}
				},
				Config:             testAccXDRDCConfigWithNamespace(),
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
			{
				Config: testAccXDRDCConfigWithNamespace(),
				Check:  testAccCheckXDRDCMembers("test-dc", []string{"aerospike"}, []string{"aerospike-target-1:3000"}),
			},
		},
	})
}

// A node detached from the DC outside Terraform shows up in the plan and is
// re-added on apply.
func TestAccAerospikeXDRDCConfig_nodeRemovedOutsideTerraform(t *testing.T) {
	nodes := []string{"aerospike-target-1:3000", "aerospike-target-2:3000"}
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithNodes(nodes),
				Check:  testAccCheckXDRDCMembers("test-dc", nil, nodes),
			},
			{
				PreConfig: func() {
					client, err := testAccGetAerospikeClient()
					if err != nil {
						t.Fatalf("failed to get client: %s", err)
					}
					defer client.Close()
					if _, err := removeXDRDCNode(client, "test-dc", "aerospike-target-2:3000"); err != nil {
						t.Fatalf("failed to remove node outside Terraform: %s", err)
					}
				},
				Config:             testAccXDRDCConfigWithNodes(nodes),
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
			{
				Config: testAccXDRDCConfigWithNodes(nodes),
				Check:  testAccCheckXDRDCMembers("test-dc", nil, nodes),
			},
		},
	})
}

// ship-only-specified-sets changed outside Terraform shows up in the plan and
// is restored on apply.
func TestAccAerospikeXDRDCConfig_shipOnlySpecifiedSetsDrift(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithShipSets(),
				Check:  testAccCheckXDRDCNamespaceParam("ship-only-specified-sets", "true"),
			},
			{
				PreConfig: func() {
					client, err := testAccGetAerospikeClient()
					if err != nil {
						t.Fatalf("failed to get client: %s", err)
					}
					defer client.Close()
					if _, err := setXDRDCNamespaceParam(client, "test-dc", "aerospike", "ship-only-specified-sets", "false"); err != nil {
						t.Fatalf("failed to change ship-only-specified-sets outside Terraform: %s", err)
					}
				},
				Config:             testAccXDRDCConfigWithShipSets(),
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
			{
				Config: testAccXDRDCConfigWithShipSets(),
				Check:  testAccCheckXDRDCNamespaceParam("ship-only-specified-sets", "true"),
			},
		},
	})
}

// Removing the whole params map applies cleanly (with a warning) and leaves
// the value on the server.
func TestAccAerospikeXDRDCConfig_removeAllDCParams(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccXDRDCConfigPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckXDRDCDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccXDRDCConfigWithDCParams("200"),
				Check:  testAccCheckXDRDCParam("test-dc", "period-ms", "200"),
			},
			{
				Config: testAccXDRDCConfigWithNodes([]string{"aerospike-target-1:3000"}),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckNoResourceAttr("aerospike_xdr_dc_config.test", "params"),
					testAccCheckXDRDCParam("test-dc", "period-ms", "200"),
				),
			},
		},
	})
}

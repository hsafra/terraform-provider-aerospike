// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// testAccCheckAerospikeRoleDestroy verifies the role has been destroyed.
func testAccCheckAerospikeRoleDestroy(s *terraform.State) error {
	client, err := testAccGetAerospikeClient()
	if err != nil {
		return err
	}
	defer (*client).Close()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aerospike_role" {
			continue
		}

		adminPol := as.NewAdminPolicy()
		_, queryErr := (*client).QueryRole(adminPol, rs.Primary.Attributes["role_name"])
		if queryErr == nil {
			return fmt.Errorf("aerospike role %s still exists", rs.Primary.Attributes["role_name"])
		}
		if !queryErr.Matches(astypes.INVALID_ROLE) {
			return fmt.Errorf("unexpected error checking role %s: %s", rs.Primary.Attributes["role_name"], queryErr)
		}
	}
	return nil
}

func TestAccAerospikeRole_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeRoleDestroy,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccAerospikeRoleConfig("testrole1", `[{privilege="read"}]`, `["1.1.1.1"]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "role_name", "testrole1"),
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "white_list.0", "1.1.1.1"),
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "read_quota", "0"),
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "write_quota", "0"),
				),
			},
			// ImportState testing
			{
				ResourceName:                         "aerospike_role.testrole1",
				ImportState:                          true,
				ImportStateVerify:                    true,
				ImportStateId:                        "testrole1",
				ImportStateVerifyIdentifierAttribute: "role_name",
			},
			// Update privileges
			{
				Config: testAccAerospikeRoleConfig("testrole1", `[{privilege="write",namespace="aerospike",set="test"}]`, `["1.1.1.1"]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "role_name", "testrole1"),
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "white_list.0", "1.1.1.1"),
				),
			},
			// Update whitelist
			{
				Config: testAccAerospikeRoleConfig("testrole1", `[{privilege="write",namespace="aerospike",set="test"}]`, `["2.2.2.2"]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "role_name", "testrole1"),
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "white_list.0", "2.2.2.2"),
				),
			},
		},
	})
}

func TestAccAerospikeRole_quotas(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeRoleDestroy,
		Steps: []resource.TestStep{
			// Create with quotas
			{
				Config: testAccAerospikeRoleWithQuotasConfig("testrole_quota", `[{privilege="read"}]`, 100, 200),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_quota", "role_name", "testrole_quota"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_quota", "read_quota", "100"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_quota", "write_quota", "200"),
				),
			},
			// Update quotas
			{
				Config: testAccAerospikeRoleWithQuotasConfig("testrole_quota", `[{privilege="read"}]`, 500, 1000),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_quota", "read_quota", "500"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_quota", "write_quota", "1000"),
				),
			},
			// Reset quotas to zero
			{
				Config: testAccAerospikeRoleWithQuotasConfig("testrole_quota", `[{privilege="read"}]`, 0, 0),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_quota", "read_quota", "0"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_quota", "write_quota", "0"),
				),
			},
		},
	})
}

func TestAccAerospikeRole_privilegeScoping(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeRoleDestroy,
		Steps: []resource.TestStep{
			// Global privilege (no namespace/set)
			{
				Config: testAccAerospikeRoleMinimalConfig("testrole_scope", `[{privilege="read"}]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_scope", "role_name", "testrole_scope"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_scope", "privileges.#", "1"),
				),
			},
			// Namespace-scoped privilege
			{
				Config: testAccAerospikeRoleMinimalConfig("testrole_scope", `[{privilege="read", namespace="aerospike"}]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_scope", "privileges.#", "1"),
				),
			},
			// Namespace+set scoped privilege
			{
				Config: testAccAerospikeRoleMinimalConfig("testrole_scope", `[{privilege="read", namespace="aerospike", set="testset"}]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_scope", "privileges.#", "1"),
				),
			},
		},
	})
}

func TestAccAerospikeRole_multiplePrivileges(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeRoleDestroy,
		Steps: []resource.TestStep{
			// Create with multiple privileges of different types and scopes
			{
				Config: testAccAerospikeRoleMinimalConfig("testrole_multi",
					`[{privilege="read"}, {privilege="write", namespace="aerospike"}, {privilege="read-write", namespace="aerospike", set="testset"}]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_multi", "role_name", "testrole_multi"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_multi", "privileges.#", "3"),
				),
			},
		},
	})
}

func TestAccAerospikeRole_privilegeTypes(t *testing.T) {
	// Test each privilege type to ensure the mapping works correctly
	privilegeTypes := []string{
		"read", "write", "read-write", "read-write-udf",
		"sys-admin", "user-admin", "data-admin", "truncate",
	}

	for _, privType := range privilegeTypes {
		t.Run(privType, func(t *testing.T) {
			roleName := fmt.Sprintf("testrole_%s", sanitizeRoleName(privType))
			resource.Test(t, resource.TestCase{
				PreCheck:                 func() { testAccPreCheck(t) },
				ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
				CheckDestroy:             testAccCheckAerospikeRoleDestroy,
				Steps: []resource.TestStep{
					{
						Config: testAccAerospikeRoleMinimalConfig(roleName,
							fmt.Sprintf(`[{privilege="%s"}]`, privType)),
						Check: resource.ComposeAggregateTestCheckFunc(
							resource.TestCheckResourceAttr(
								fmt.Sprintf("aerospike_role.%s", roleName), "role_name", roleName),
						),
					},
				},
			})
		})
	}
}

func TestAccAerospikeRole_whitelistManagement(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeRoleDestroy,
		Steps: []resource.TestStep{
			// Create with whitelist
			{
				Config: testAccAerospikeRoleConfig("testrole_wl", `[{privilege="read"}]`, `["10.0.0.1", "10.0.0.2"]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_wl", "white_list.#", "2"),
				),
			},
			// Update to different whitelist
			{
				Config: testAccAerospikeRoleConfig("testrole_wl", `[{privilege="read"}]`, `["192.168.1.0/24"]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_wl", "white_list.#", "1"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_wl", "white_list.0", "192.168.1.0/24"),
				),
			},
			// Remove whitelist entirely (omit white_list from config)
			{
				Config: testAccAerospikeRoleMinimalConfig("testrole_wl", `[{privilege="read"}]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckNoResourceAttr("aerospike_role.testrole_wl", "white_list.0"),
				),
			},
		},
	})
}

// testAccAerospikeRoleMinimalConfig creates a role config without white_list.
// When white_list is omitted from config and the role has no whitelist,
// Read returns nil which matches the absent attribute — no plan diff.
func testAccAerospikeRoleMinimalConfig(roleName string, privileges string) string {
	return fmt.Sprintf(`
resource "aerospike_role" "%[1]s" {
  role_name   = "%[1]s"
  privileges  = %[2]s
}`, roleName, privileges)
}

func testAccAerospikeRoleConfig(roleName string, privileges string, whiteList string) string {
	return fmt.Sprintf(`
resource "aerospike_role" "%[1]s" {
  role_name   = "%[1]s"
  privileges  = %[2]s
  white_list  = %[3]s
}`, roleName, privileges, whiteList)
}

func testAccAerospikeRoleWithQuotasConfig(roleName string, privileges string, readQuota int, writeQuota int) string {
	return fmt.Sprintf(`
resource "aerospike_role" "%[1]s" {
  role_name   = "%[1]s"
  privileges  = %[2]s
  read_quota  = %[3]d
  write_quota = %[4]d
}`, roleName, privileges, readQuota, writeQuota)
}

// sanitizeRoleName replaces hyphens with underscores for valid Terraform resource names.
func sanitizeRoleName(name string) string {
	result := make([]byte, len(name))
	for i := range name {
		if name[i] == '-' {
			result[i] = '_'
		} else {
			result[i] = name[i]
		}
	}
	return string(result)
}

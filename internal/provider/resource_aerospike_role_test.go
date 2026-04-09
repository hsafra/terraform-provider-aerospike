// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"regexp"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// testAccCheckAerospikeRoleDestroy verifies the role has been destroyed.
func testAccCheckAerospikeRoleDestroy(s *terraform.State) error {
	client, err := testAccGetAerospikeClient()
	if err != nil {
		return err
	}
	defer client.Close()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aerospike_role" {
			continue
		}

		adminPol := as.NewAdminPolicy()
		_, queryErr := client.QueryRole(adminPol, rs.Primary.Attributes["role_name"])
		if queryErr == nil {
			return fmt.Errorf("aerospike role %s still exists", rs.Primary.Attributes["role_name"])
		}
		if !queryErr.Matches(astypes.INVALID_ROLE) {
			return fmt.Errorf("unexpected error checking role %s: %w", rs.Primary.Attributes["role_name"], queryErr)
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
		"udf-admin", "sindex-admin",
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

// #6: Changing role_name forces replacement.
func TestAccAerospikeRole_replaceOnNameChange(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAerospikeRoleConfigNamed("role_replace", "testrole_x", `[{privilege="read"}]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.role_replace", "role_name", "testrole_x"),
					testAccCheckRoleExists("testrole_x"),
				),
			},
			// Change role_name — should destroy testrole_x and create testrole_y
			{
				Config: testAccAerospikeRoleConfigNamed("role_replace", "testrole_y", `[{privilege="read"}]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.role_replace", "role_name", "testrole_y"),
					testAccCheckRoleExists("testrole_y"),
					testAccCheckRoleNotExists("testrole_x"),
				),
			},
		},
	})
}

// #7: Set without namespace should produce a validation error.
func TestAccAerospikeRole_setWithoutNamespace(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccAerospikeRoleMinimalConfig("testrole_setns", `[{privilege="read", set="testset"}]`),
				ExpectError: regexp.MustCompile(`(?i)also requires|namespace`),
			},
		},
	})
}

// #8: Invalid privilege name should produce a validation error.
func TestAccAerospikeRole_invalidPrivilegeName(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccAerospikeRoleMinimalConfig("testrole_badpriv", `[{privilege="superadmin"}]`),
				ExpectError: regexp.MustCompile(`(?i)invalid|value must be one of`),
			},
		},
	})
}

// #9: Role referencing a non-existent namespace.
func TestAccAerospikeRole_invalidNamespace(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccAerospikeRoleMinimalConfig("testrole_badns", `[{privilege="read", namespace="nonexistent_ns"}]`),
				ExpectError: regexp.MustCompile(`Invalid namesace|does not exist`),
			},
		},
	})
}

// #10: Role that already exists on the server.
func TestAccAerospikeRole_roleAlreadyExists(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			// Pre-create the role outside Terraform
			client, err := testAccGetAerospikeClient()
			if err != nil {
				t.Fatalf("failed to get client: %s", err)
			}
			defer client.Close()
			adminPol := as.NewAdminPolicy()
			_ = client.CreateRole(adminPol, "testrole_preexist", []as.Privilege{
				{Code: as.Read},
			}, nil, 0, 0)
		},
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy: func(s *terraform.State) error {
			// Clean up the pre-created role
			client, err := testAccGetAerospikeClient()
			if err != nil {
				return err
			}
			defer client.Close()
			adminPol := as.NewAdminPolicy()
			_ = client.DropRole(adminPol, "testrole_preexist")
			return nil
		},
		Steps: []resource.TestStep{
			{
				Config:      testAccAerospikeRoleMinimalConfig("testrole_preexist", `[{privilege="read"}]`),
				ExpectError: regexp.MustCompile(`Role already exists`),
			},
		},
	})
}

// #11: Role disappears outside Terraform.
func TestAccAerospikeRole_disappearsOutsideTerraform(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAerospikeRoleMinimalConfig("testrole_disappear", `[{privilege="read"}]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_disappear", "role_name", "testrole_disappear"),
				),
			},
			// Drop role outside Terraform, then re-apply — should recreate
			{
				PreConfig: func() {
					client, err := testAccGetAerospikeClient()
					if err != nil {
						t.Fatalf("failed to get client: %s", err)
					}
					defer client.Close()
					adminPol := as.NewAdminPolicy()
					_ = client.DropRole(adminPol, "testrole_disappear")
				},
				Config: testAccAerospikeRoleMinimalConfig("testrole_disappear", `[{privilege="read"}]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_disappear", "role_name", "testrole_disappear"),
					testAccCheckRoleExists("testrole_disappear"),
				),
			},
		},
	})
}

// #12: Update privileges, quotas, and whitelist all at once.
func TestAccAerospikeRole_updatePrivilegesAndQuotasTogether(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAerospikeRoleFullConfig("testrole_all", `[{privilege="read"}]`, `["1.1.1.1"]`, 100, 200),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_all", "read_quota", "100"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_all", "write_quota", "200"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_all", "white_list.0", "1.1.1.1"),
				),
			},
			// Update everything simultaneously
			{
				Config: testAccAerospikeRoleFullConfig("testrole_all", `[{privilege="write", namespace="aerospike"}]`, `["2.2.2.2", "3.3.3.3"]`, 500, 1000),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_all", "read_quota", "500"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_all", "write_quota", "1000"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_all", "white_list.#", "2"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_all", "privileges.#", "1"),
				),
			},
		},
	})
}

// #13: Import with quotas, whitelist, and multiple privileges — full round-trip.
func TestAccAerospikeRole_importVerifyAllFields(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAerospikeRoleFullConfig("testrole_import", `[{privilege="read"}, {privilege="write", namespace="aerospike"}]`, `["10.0.0.1"]`, 100, 200),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole_import", "role_name", "testrole_import"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_import", "read_quota", "100"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_import", "write_quota", "200"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_import", "white_list.#", "1"),
					resource.TestCheckResourceAttr("aerospike_role.testrole_import", "privileges.#", "2"),
				),
			},
			{
				ResourceName:                         "aerospike_role.testrole_import",
				ImportState:                          true,
				ImportStateVerify:                    true,
				ImportStateId:                        "testrole_import",
				ImportStateVerifyIdentifierAttribute: "role_name",
			},
		},
	})
}

// testAccCheckRoleExists verifies a role exists on the server.
func testAccCheckRoleExists(roleName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		adminPol := as.NewAdminPolicy()
		_, queryErr := client.QueryRole(adminPol, roleName)
		if queryErr != nil {
			return fmt.Errorf("role %s does not exist: %w", roleName, queryErr)
		}
		return nil
	}
}

// testAccCheckRoleNotExists verifies a role does NOT exist on the server.
func testAccCheckRoleNotExists(roleName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		adminPol := as.NewAdminPolicy()
		_, queryErr := client.QueryRole(adminPol, roleName)
		if queryErr == nil {
			return fmt.Errorf("role %s still exists, expected it to be deleted", roleName)
		}
		if !queryErr.Matches(astypes.INVALID_ROLE) {
			return fmt.Errorf("unexpected error checking role %s: %w", roleName, queryErr)
		}
		return nil
	}
}

func testAccAerospikeRoleConfigNamed(resourceName, roleName, privileges string) string {
	return fmt.Sprintf(`
resource "aerospike_role" "%[1]s" {
  role_name   = "%[2]s"
  privileges  = %[3]s
}`, resourceName, roleName, privileges)
}

func testAccAerospikeRoleFullConfig(roleName, privileges, whiteList string, readQuota, writeQuota int) string {
	return fmt.Sprintf(`
resource "aerospike_role" "%[1]s" {
  role_name   = "%[1]s"
  privileges  = %[2]s
  white_list  = %[3]s
  read_quota  = %[4]d
  write_quota = %[5]d
}`, roleName, privileges, whiteList, readQuota, writeQuota)
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

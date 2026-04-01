// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// testAccCheckAerospikeUserDestroy verifies the user has been destroyed.
func testAccCheckAerospikeUserDestroy(s *terraform.State) error {
	client, err := testAccGetAerospikeClient()
	if err != nil {
		return err
	}
	defer client.Close()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aerospike_user" {
			continue
		}

		adminPol := as.NewAdminPolicy()
		_, queryErr := client.QueryUser(adminPol, rs.Primary.Attributes["user_name"])
		if queryErr == nil {
			return fmt.Errorf("aerospike user %s still exists", rs.Primary.Attributes["user_name"])
		}
		if !queryErr.Matches(astypes.INVALID_USER) {
			return fmt.Errorf("unexpected error checking user %s: %s", rs.Primary.Attributes["user_name"], queryErr)
		}
	}
	return nil
}

func TestAccAerospikeUser_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccAerospikeUserConfig("testuser1", "testpass1", `"read"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "user_name", "testuser1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "password", "testpass1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "roles.#", "1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "roles.0", "read"),
				),
			},
			// ImportState testing
			{
				ResourceName:                         "aerospike_user.testuser1",
				ImportState:                          true,
				ImportStateVerify:                    true,
				ImportStateVerifyIgnore:              []string{"password"},
				ImportStateId:                        "testuser1",
				ImportStateVerifyIdentifierAttribute: "user_name",
			},
			// Update password
			{
				Config: testAccAerospikeUserConfig("testuser1", "testpass2", `"read"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "user_name", "testuser1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "password", "testpass2"),
				),
			},
		},
	})
}

func TestAccAerospikeUser_roleUpdates(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			// Create with one role
			{
				Config: testAccAerospikeUserConfig("testuser_roles", "testpass1", `"read"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_roles", "roles.#", "1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser_roles", "roles.0", "read"),
				),
			},
			// Grant additional role
			{
				Config: testAccAerospikeUserConfig("testuser_roles", "testpass1", `"read", "write"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_roles", "roles.#", "2"),
				),
			},
			// Revoke a role (keep only write)
			{
				Config: testAccAerospikeUserConfig("testuser_roles", "testpass1", `"write"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_roles", "roles.#", "1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser_roles", "roles.0", "write"),
				),
			},
		},
	})
}

func TestAccAerospikeUser_multipleRoles(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			// Create with multiple roles at once (alphabetical order to match server response)
			{
				Config: testAccAerospikeUserConfig("testuser_multi", "testpass1", `"read", "sys-admin", "write"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_multi", "roles.#", "3"),
				),
			},
		},
	})
}

func TestAccAerospikeUser_noRoles(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAerospikeUserConfigNoRoles("testuser_noroles", "testpass1"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_noroles", "user_name", "testuser_noroles"),
				),
			},
		},
	})
}

func TestAccAerospikeUser_withCustomRole(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			// Create a custom role and a user that references it
			{
				Config: testAccAerospikeUserWithCustomRoleConfig(),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.custom_for_user", "role_name", "custom_for_user"),
					resource.TestCheckResourceAttr("aerospike_user.user_with_custom", "user_name", "user_with_custom"),
					resource.TestCheckResourceAttr("aerospike_user.user_with_custom", "roles.#", "1"),
					resource.TestCheckResourceAttr("aerospike_user.user_with_custom", "roles.0", "custom_for_user"),
				),
			},
		},
	})
}

// testAccCheckUserHasRoles verifies that the user exists on the server with the expected roles.
func testAccCheckUserHasRoles(userName string, expectedRoles []string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		adminPol := as.NewAdminPolicy()
		user, queryErr := client.QueryUser(adminPol, userName)
		if queryErr != nil {
			return fmt.Errorf("failed to query user %s: %s", userName, queryErr)
		}

		actualSet := make(map[string]bool)
		for _, r := range user.Roles {
			if r != "" {
				actualSet[r] = true
			}
		}

		expectedSet := make(map[string]bool)
		for _, r := range expectedRoles {
			expectedSet[r] = true
		}

		if len(actualSet) != len(expectedSet) {
			return fmt.Errorf("user %s: expected roles %v, got %v", userName, expectedRoles, user.Roles)
		}
		for r := range expectedSet {
			if !actualSet[r] {
				return fmt.Errorf("user %s: expected role %q not found, got %v", userName, r, user.Roles)
			}
		}
		return nil
	}
}

// #5: Server-side role validation for users.
func TestAccAerospikeUser_serverSideRoleValidation(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAerospikeUserConfig("testuser_srv", "testpass1", `"read", "write"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_srv", "user_name", "testuser_srv"),
					testAccCheckUserHasRoles("testuser_srv", []string{"read", "write"}),
				),
			},
			// Update roles and verify server-side
			{
				Config: testAccAerospikeUserConfig("testuser_srv", "testpass1", `"sys-admin"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckUserHasRoles("testuser_srv", []string{"sys-admin"}),
				),
			},
		},
	})
}

// #2: Update both password and roles simultaneously.
func TestAccAerospikeUser_passwordAndRoleUpdateTogether(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAerospikeUserConfig("testuser_both", "pass1", `"read"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_both", "password", "pass1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser_both", "roles.#", "1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser_both", "roles.0", "read"),
					testAccCheckUserHasRoles("testuser_both", []string{"read"}),
				),
			},
			// Update password AND roles in one step
			{
				Config: testAccAerospikeUserConfig("testuser_both", "pass2", `"write", "sys-admin"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_both", "password", "pass2"),
					resource.TestCheckResourceAttr("aerospike_user.testuser_both", "roles.#", "2"),
					testAccCheckUserHasRoles("testuser_both", []string{"write", "sys-admin"}),
				),
			},
		},
	})
}

// #3: Revoke all roles (empty roles list).
func TestAccAerospikeUser_revokeAllRoles(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			// Create with roles
			{
				Config: testAccAerospikeUserConfig("testuser_revoke", "testpass1", `"read", "write"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_revoke", "roles.#", "2"),
					testAccCheckUserHasRoles("testuser_revoke", []string{"read", "write"}),
				),
			},
			// Revoke all roles by setting empty list
			{
				Config: testAccAerospikeUserConfigEmptyRoles("testuser_revoke", "testpass1"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_revoke", "roles.#", "0"),
					testAccCheckUserHasRoles("testuser_revoke", []string{}),
				),
			},
		},
	})
}

// #4: User disappears outside Terraform.
func TestAccAerospikeUser_disappearsOutsideTerraform(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			// Create the user
			{
				Config: testAccAerospikeUserConfig("testuser_disappear", "testpass1", `"read"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_disappear", "user_name", "testuser_disappear"),
				),
			},
			// Delete user outside Terraform, then run plan — should recreate
			{
				PreConfig: func() {
					client, err := testAccGetAerospikeClient()
					if err != nil {
						t.Fatalf("failed to get client: %s", err)
					}
					defer client.Close()
					adminPol := as.NewAdminPolicy()
					_ = client.DropUser(adminPol, "testuser_disappear")
				},
				Config: testAccAerospikeUserConfig("testuser_disappear", "testpass1", `"read"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser_disappear", "user_name", "testuser_disappear"),
					testAccCheckUserHasRoles("testuser_disappear", []string{"read"}),
				),
			},
		},
	})
}

// #1: Changing user_name forces replacement.
func TestAccAerospikeUser_replaceOnNameChange(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAerospikeUserConfigNamed("user_replace", "testuser_a", "testpass1", `"read"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.user_replace", "user_name", "testuser_a"),
					testAccCheckUserHasRoles("testuser_a", []string{"read"}),
				),
			},
			// Change user_name — should destroy testuser_a and create testuser_b
			{
				Config: testAccAerospikeUserConfigNamed("user_replace", "testuser_b", "testpass1", `"read"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.user_replace", "user_name", "testuser_b"),
					testAccCheckUserHasRoles("testuser_b", []string{"read"}),
					// Verify old user is gone
					testAccCheckUserNotExists("testuser_a"),
				),
			},
		},
	})
}

// testAccCheckUserNotExists verifies a user does NOT exist on the server.
func testAccCheckUserNotExists(userName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		adminPol := as.NewAdminPolicy()
		_, queryErr := client.QueryUser(adminPol, userName)
		if queryErr == nil {
			return fmt.Errorf("user %s still exists, expected it to be deleted", userName)
		}
		if !queryErr.Matches(astypes.INVALID_USER) {
			return fmt.Errorf("unexpected error checking user %s: %s", userName, queryErr)
		}
		return nil
	}
}

func testAccAerospikeUserConfigEmptyRoles(userName, password string) string {
	return fmt.Sprintf(`
resource "aerospike_user" "%[1]s" {
  user_name = "%[1]s"
  password  = "%[2]s"
  roles     = []
}`, userName, password)
}

func testAccAerospikeUserConfigNamed(resourceName, userName, password, roles string) string {
	return fmt.Sprintf(`
resource "aerospike_user" "%[1]s" {
  user_name = "%[2]s"
  password  = "%[3]s"
  roles     = [%[4]s]
}`, resourceName, userName, password, roles)
}

func testAccAerospikeUserConfig(userName string, password string, roles string) string {
	return fmt.Sprintf(`
resource "aerospike_user" "%[1]s" {
  user_name = "%[1]s"
  password  = "%[2]s"
  roles     = [%[3]s]
}`, userName, password, roles)
}

func testAccAerospikeUserConfigNoRoles(userName string, password string) string {
	return fmt.Sprintf(`
resource "aerospike_user" "%[1]s" {
  user_name = "%[1]s"
  password  = "%[2]s"
}`, userName, password)
}

func testAccAerospikeUserWithCustomRoleConfig() string {
	return `
resource "aerospike_role" "custom_for_user" {
  role_name  = "custom_for_user"
  privileges = [{privilege="read", namespace="aerospike"}]
}

resource "aerospike_user" "user_with_custom" {
  user_name = "user_with_custom"
  password  = "testpass1"
  roles     = [aerospike_role.custom_for_user.role_name]
}
`
}

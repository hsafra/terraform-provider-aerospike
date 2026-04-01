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

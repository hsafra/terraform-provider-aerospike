// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccAerospikeRole(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccAerospikeRoleConfig("testrole1", "[{privilege=\"read\"}]", "[\"1.1.1.1\"]"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "role_name", "testrole1"),
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "white_list.0", "1.1.1.1"),
				),
			},
			// update privs
			{
				Config: testAccAerospikeRoleConfig("testrole1", "[{privilege=\"write\",namespace=\"aerospike\",set=\"test\"}]", "[\"1.1.1.1\"]"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "role_name", "testrole1"),
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "white_list.0", "1.1.1.1"),
				),
			},
			// update white list
			{
				Config: testAccAerospikeRoleConfig("testrole1", "[{privilege=\"write\",namespace=\"aerospike\",set=\"test\"}]", "[\"2.2.2.2\"]"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "role_name", "testrole1"),
					resource.TestCheckResourceAttr("aerospike_role.testrole1", "white_list.0", "2.2.2.2"),
				),
			},
		},
	})
}

func testAccAerospikeRoleConfig(roleName string, privileges string, white_list string) string {
	return fmt.Sprintf(`
resource "aerospike_role" "%[1]s" {
  role_name   = "%[1]s"
  privileges  = %[2]s
  white_list  = %[3]s
}`, roleName, privileges, white_list)
}

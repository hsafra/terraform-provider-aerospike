// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccAerospikeUser(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccAerospikeUserConfig("testuser1", "testpass1", "\"role1\""),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "user_name", "testuser1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "password", "testpass1"),
				),
			},
			// update password
			{
				Config: testAccAerospikeUserConfig("testuser1", "testpass2", "\"role1\""),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "user_name", "testuser1"),
					resource.TestCheckResourceAttr("aerospike_user.testuser1", "password", "testpass2"),
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
  roles = [%[3]s]
}`, userName, password, roles)
}

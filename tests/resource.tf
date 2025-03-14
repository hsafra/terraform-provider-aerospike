#resource "aerospike_user" "test2" {
#  user_name = "test2"
#  password  = "test24"
#  roles     = ["role21", "role22"]
#}
#
#resource "aerospike_user" "test3" {
# user_name = "test3"
# password  = "test3"
# roles     = ["role1"]
#}

# resource "aerospike_role" "role1" {
#   role_name = "role1"
#   privileges = [
#     {
#       privilege = "read"
#       namespace = "aerospike"
#       set       = "test"
#     }
#   ]
# }

#resource "aerospike_role" "role2" {
#  role_name = "role2"
#  privileges = [
#    {
#      privilege = "read"
#    }
#  ]
#}
#
#resource "aerospike_role" "role3" {
#  role_name = "role3"
#  privileges = [
#    {
#      privilege = "read"
#    },
#    {
#      privilege = "sys-admin"
#    },
#    { privilege = "read-write", namespace = "aerospike", set = "harel" }
#  ]
#}
#
#resource "aerospike_role" "role6" {
#  role_name  = "role6"
#  privileges = [{ privilege = "read-write" }]
#  white_list = ["1.1.1.5", "3.3.3.3"]
#}

resource "aerospike_namespace_config" "aerospike_ns" {
  namespace = "aerospike"
  default_set_ttl = {
    "set1" = "100"
    "set2" = "10M"
  }
  migartion_threads = 2
  xdr_datacenter    = "DC1"
  xdr_exclude       = ["set3"]
}

output "info_commands" {
  value = aerospike_namespace_config.aerospike_ns.info_commands
}

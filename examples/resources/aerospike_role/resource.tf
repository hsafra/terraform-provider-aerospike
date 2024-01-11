resource "aerospike_role" "role2" {
  role_name = "role2"
  privileges = [
    {
      privilege = "read"
      namespace = "aerospike"
      set       = "set1"
    }
  ]
  read_quota = 10
}

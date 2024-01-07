resource "aerospike_user" "example" {
  user_name = "test2"
  password  = "test24"
  roles = ["role1","role2"]
}

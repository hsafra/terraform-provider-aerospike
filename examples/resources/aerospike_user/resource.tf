resource "aerospike_user" "test2" {
  user_name = "test2"
  password  = "test24"
  roles     = ["role21", "role22"]
}
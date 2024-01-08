resource "aerospike_user" "test2" {
  user_name = "test2"
  password  = "test24"
  roles = ["role21","role22"]
}

resource "aerospike_user" "test3" {
  user_name = "test3"
  password  = "test3"
  roles = ["role31","role32"]
}

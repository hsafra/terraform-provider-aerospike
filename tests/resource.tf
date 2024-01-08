resource "aerospike_user" "test2" {
  user_name = "test2"
  password  = "test24"
  roles     = ["role21", "role22"]
}

resource "aerospike_user" "test3" {
  user_name = "test3"
  password  = "test3"
  roles     = ["role31", "role32"]
}

resource "aerospike_role" "role1" {
  role_name = "role1"
  privileges = [
    {
      privilege = "read"
    }
  ]
}

resource "aerospike_role" "role2" {
  role_name = "role2"
  privileges = [
    {
      privilege = "read"
      namespace="namespace1"
      set="set1"
    }
  ]
}
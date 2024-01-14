provider "aerospike" {
  host      = "localhost"
  user_name = "admin"
  password  = "admin"
  port      = 4333
  tls = {
    tls_name = "aerospike"
    root_ca_file="/Users/harel.safra/docker/aerospike_with_security/server.crt"
  }
}

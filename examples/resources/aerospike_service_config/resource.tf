# Manage service-level configuration parameters
resource "aerospike_service_config" "example" {
  params = {
    "proto-fd-max" = "20000"
  }
}

# Access the list of asinfo commands that were executed
output "applied_commands" {
  value = aerospike_service_config.example.info_commands
}

# Manage a set index (Aerospike Database 8.1.2+)
# Same-apply enable-index changes on this set need depends_on vs aerospike_namespace_config.
resource "aerospike_sindex" "set1" {
  namespace  = "aerospike"
  set        = "set1"
  name       = "set1-idx"
  index_type = "set"
}

output "sindex_commands" {
  value = aerospike_sindex.set1.info_commands
}

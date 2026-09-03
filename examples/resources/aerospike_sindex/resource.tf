# Manage a set index (Aerospike Database 8.1.2+)
resource "aerospike_sindex" "jobs" {
  namespace  = "aerospike"
  set        = "shuttlex_jobs"
  name       = "shuttlex_jobs-set-idx"
  index_type = "set"
}

output "sindex_commands" {
  value = aerospike_sindex.jobs.info_commands
}

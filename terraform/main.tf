# Create a volume for storing init scripts
resource "databricks_volume" "init_scripts" {
  catalog_name = var.catalog
  schema_name  = var.schema
  name         = "init_scripts"
  volume_type  = "MANAGED"
  comment      = "Init scripts for Watchtower"
}

resource "databricks_file" "init_script_configure_log4j" {
  path   = "${databricks_volume.init_scripts.volume_path}/configure_log4j.sh"
  source = "${path.module}/init-scripts/configure_log4j.sh"
}

# Create a volume for storing raw cluster logs.
resource "databricks_volume" "raw_log_storage" {
  catalog_name = var.catalog
  schema_name  = var.schema
  name         = "cluster_logs"
  volume_type  = "MANAGED"
  comment      = "Raw cluster logs for Watchtower"
}

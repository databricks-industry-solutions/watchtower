variable "catalog" {
  type        = string
  description = "The name of the catalog to create the volume in."
  default     = "watchtower"
}

variable "schema" {
  type        = string
  description = "The name of the schema to create the volume in."
  default     = "default"
}
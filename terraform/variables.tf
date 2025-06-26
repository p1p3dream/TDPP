variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "tag" {
  description = "Deployment tag appended to resource names"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev or prod)"
  type        = string
}

variable "source_bucket_name" {
  description = "Bucket used to store the repo"
  type        = string
}

variable "glue_assets_bucket_name" {
  description = "Bucket for glue runtime logs"
  type        = string
}

variable "saphana_glue_connection" {
  description = "Name of the SAP HANA Glue connection"
  type        = string
}

variable "bronze_bucket" { type = string }
variable "silver_bucket" { type = string }
variable "gold_bucket"   { type = string }
variable "finance_bucket" { type = string }
variable "repsly_bucket" { type = string }
variable "repsly_dynamo_table" { type = string }
variable "tdp_sf_db" { type = string }
variable "sap_hana_layer" { type = string }
variable "snowflake_layer" { type = string }
variable "waitly_live_zip" {
  description = "Path to the zipped waitly live lambda code"
  type        = string
  default     = "../waitly-live-api-daily.zip"
}

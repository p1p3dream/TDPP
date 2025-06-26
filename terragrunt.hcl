terraform {
  source = "./terraform"
}

inputs = {
  aws_region             = "us-east-1"
  tag                    = getenv("TF_VAR_tag", "cicdexec")
  environment            = getenv("TF_VAR_environment", "dev")
  source_bucket_name     = getenv("TF_VAR_source_bucket_name", "")
  glue_assets_bucket_name = getenv("TF_VAR_glue_assets_bucket_name", "")
  saphana_glue_connection = getenv("TF_VAR_saphana_glue_connection", "")
  bronze_bucket          = getenv("TF_VAR_bronze_bucket", "")
  silver_bucket          = getenv("TF_VAR_silver_bucket", "")
  gold_bucket            = getenv("TF_VAR_gold_bucket", "")
  finance_bucket         = getenv("TF_VAR_finance_bucket", "")
  repsly_bucket          = getenv("TF_VAR_repsly_bucket", "")
  repsly_dynamo_table    = getenv("TF_VAR_repsly_dynamo_table", "")
  tdp_sf_db              = getenv("TF_VAR_tdp_sf_db", "")
  sap_hana_layer         = getenv("TF_VAR_sap_hana_layer", "")
  snowflake_layer        = getenv("TF_VAR_snowflake_layer", "")
  dat_utilities_zip      = getenv("TF_VAR_dat_utilities_zip", "../dat_utilities.zip")
  waitly_live_zip        = getenv("TF_VAR_waitly_live_zip", "../waitly-live-api-daily.zip")
}

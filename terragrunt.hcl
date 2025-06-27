terraform {
  source = "./terraform"
}

remote_state {
  backend = "s3"
  generate = {
    path = "backend.tf"
  }
  config = {
    bucket         = get_env("TF_STATE_BUCKET", "tdp-terraform-state")
    key            = "terraform/${get_env("TF_VAR_environment", "dev")}/terraform.tfstate"
    region         = get_env("AWS_REGION", "us-east-1")
    encrypt        = true
    dynamodb_table = "terraform-locks"
  }
}

inputs = {
  aws_region             = "us-east-1"
  tag                    = get_env("TF_VAR_tag", "cicdexec")
  environment            = get_env("TF_VAR_environment", "dev")
  source_bucket_name     = get_env("TF_VAR_source_bucket_name", "")
  glue_assets_bucket_name = get_env("TF_VAR_glue_assets_bucket_name", "")
  saphana_glue_connection = get_env("TF_VAR_saphana_glue_connection", "")
  bronze_bucket          = get_env("TF_VAR_bronze_bucket", "")
  silver_bucket          = get_env("TF_VAR_silver_bucket", "")
  gold_bucket            = get_env("TF_VAR_gold_bucket", "")
  finance_bucket         = get_env("TF_VAR_finance_bucket", "")
  repsly_bucket          = get_env("TF_VAR_repsly_bucket", "")
  repsly_dynamo_table    = get_env("TF_VAR_repsly_dynamo_table", "")
  tdp_sf_db              = get_env("TF_VAR_tdp_sf_db", "")
  sap_hana_layer         = get_env("TF_VAR_sap_hana_layer", "")
  snowflake_layer        = get_env("TF_VAR_snowflake_layer", "")
  dat_utilities_zip      = get_env("TF_VAR_dat_utilities_zip", "../dat_utilities.zip")
  waitly_live_zip        = get_env("TF_VAR_waitly_live_zip", "../waitly-live-api-daily.zip")
}

module "waitly" {
  source = "./modules/waitly"

  tag            = var.tag
  bronze_bucket  = var.bronze_bucket
  waitly_live_zip = var.waitly_live_zip
  lambda_role_arn = aws_iam_role.lambda_role.arn
}

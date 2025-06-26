variable "tag" {
  description = "Deployment tag appended to resource names"
  type        = string
}

variable "bronze_bucket" {
  description = "Bronze bucket used by the lambda"
  type        = string
}

variable "waitly_live_zip" {
  description = "Path to the zipped waitly live lambda code"
  type        = string
}

variable "lambda_role_arn" {
  description = "IAM role ARN for the Waitly lambda"
  type        = string
}

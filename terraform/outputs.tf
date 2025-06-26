output "glue_job_role_arn" {
  value = aws_iam_role.glue_job_role.arn
}

output "lambda_role_arn" {
  value = aws_iam_role.lambda_role.arn
}

output "comprehensive_role_arn" {
  value = aws_iam_role.comprehensive_role.arn
}

resource "aws_iam_role" "glue_job_role" {
  name = "GlueRole-${var.tag}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = ["glue.amazonaws.com", "lambda.amazonaws.com"] }
      Action = "sts:AssumeRole"
    }]
  })

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
    "arn:aws:iam::aws:policy/SecretsManagerReadWrite",
  ]

  inline_policy {
    name   = "GluePolicy"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Effect   = "Allow"
          Action   = ["s3:*"]
          Resource = [
            "arn:aws:s3:::${var.glue_assets_bucket_name}*",
            "arn:aws:s3:::${var.source_bucket_name}*"
          ]
        },
        {
          Effect   = "Allow"
          Action   = ["glue:*"]
          Resource = "*"
        },
        {
          Effect   = "Allow"
          Action   = ["secretsmanager:*"]
          Resource = "*"
        }
      ]
    })
  }
}

resource "aws_iam_role" "lambda_role" {
  name = "LambdaRole-${var.tag}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  inline_policy {
    name   = "LambdaRole"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Effect   = "Allow"
          Action   = ["s3:*", "secretsmanager:*"]
          Resource = "*"
        },
        {
          Effect   = "Allow"
          Action   = ["dynamodb:*"]
          Resource = "*"
        }
      ]
    })
  }
}

resource "aws_iam_role" "state_machine_execution_role" {
  name = "StateMachineExecutionRole-${var.tag}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = ["states.amazonaws.com", "lambda.amazonaws.com"] }
      Action = "sts:AssumeRole"
    }]
  })

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]

  inline_policy {
    name   = "StepFunctionPolicy"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [{
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "*"
      }]
    })
  }
}

resource "aws_iam_role" "comprehensive_role" {
  name = "ComprehensiveRole-${var.tag}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = [
        "states.amazonaws.com",
        "glue.amazonaws.com",
        "lambda.amazonaws.com",
        "dynamodb.amazonaws.com",
        "scheduler.amazonaws.com"
      ] }
      Action = "sts:AssumeRole"
    }]
  })

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AWSLambda_FullAccess",
    "arn:aws:iam::aws:policy/AdministratorAccess"
  ]

  inline_policy {
    name   = "ComprehensiveRolePolicy"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Effect   = "Allow"
          Action   = [
            "glue:*", "s3:*", "rds:*", "ec2:*", "redshift:*", "lambda:*",
            "logs:*", "states:*", "events:*", "athena:*", "secretsmanager:*",
            "ses:*", "kinesis:*", "sqs:*", "firehose:*", "dynamodb:*",
            "cloudwatch:*", "databrew:*", "aws-marketplace:*"
          ]
          Resource = "*"
        },
        {
          Effect   = "Allow"
          Action   = "lambda:InvokeFunction"
          Resource = "arn:aws:lambda:*:${data.aws_caller_identity.current.account_id}:function/*"
        }
      ]
    })
  }
}

data "aws_caller_identity" "current" {}

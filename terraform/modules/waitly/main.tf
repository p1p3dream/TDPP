resource "aws_lambda_function" "waitly_live" {
  function_name = "waitly-live-api-daily-${var.tag}"
  handler       = "waitly-live-api-daily.lambda_handler"
  runtime       = "python3.9"
  role          = var.lambda_role_arn

  filename         = var.waitly_live_zip
  source_code_hash = filebase64sha256(var.waitly_live_zip)

  timeout     = 180
  memory_size = 128
  architectures = ["x86_64"]

  environment {
    variables = {
      LOG_LEVEL     = "INFO"
      BRONZE_BUCKET = var.bronze_bucket
    }
  }
}

resource "aws_cloudwatch_event_rule" "waitly_live" {
  name                = "waitlylive-${var.tag}"
  schedule_expression = "rate(1 day)"
  is_enabled          = true
}

resource "aws_cloudwatch_event_target" "waitly_live" {
  rule      = aws_cloudwatch_event_rule.waitly_live.name
  target_id = "WaitlyLiveLambdaTarget"
  arn       = aws_lambda_function.waitly_live.arn
}

resource "aws_lambda_permission" "allow_eventbridge_waitly_live" {
  statement_id  = "AllowExecutionFromCloudWatchEvents"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.waitly_live.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.waitly_live.arn
}

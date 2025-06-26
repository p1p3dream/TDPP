resource "aws_lambda_layer_version" "dat_utilities" {
  filename          = var.dat_utilities_zip
  layer_name        = "dat-utilities-layer-${var.tag}"
  compatible_runtimes = ["python3.9"]
}

variable "dat_utilities_zip" {
  description = "Path to the zipped dat utilities layer"
  type        = string
  default     = "../dat_layer_build/dat_utilities.zip"
}

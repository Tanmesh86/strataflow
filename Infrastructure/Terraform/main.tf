data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name = "PulseFlow Data Lake"
  }
}

locals {
  data_lake_prefixes = [
    "bronze/",
    "silver/",
    "gold/",
    "quarantine/",
    "checkpoints/"
  ]
}

resource "aws_s3_object" "data_lake_prefixes" {
  for_each = toset(local.data_lake_prefixes)

  bucket = aws_s3_bucket.data_lake.id
  key    = each.value
}
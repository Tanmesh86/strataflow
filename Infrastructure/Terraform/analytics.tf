resource "aws_glue_catalog_database" "strataflow_catalog" {
  name        = "strataflow_catalog"
  description = "StrataFlow data lake catalog for analytics"
  catalog_id  = data.aws_caller_identity.current.account_id
}

resource "aws_athena_workgroup" "strataflow" {
  name        = "strataflow-analytics"
  description = "Athena workgroup for StrataFlow data lake queries"
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Glue crawler execution role
data "aws_iam_policy_document" "glue_crawler_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "strataflow_glue_crawler" {
  name               = "StrataFlowGlueCrawlerRole"
  assume_role_policy = data.aws_iam_policy_document.glue_crawler_trust.json
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.strataflow_glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_crawler_s3_read" {
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data_lake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["gold_v2", "gold_v2/*"]
    }
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject"
    ]
    resources = [
      "${aws_s3_bucket.data_lake.arn}/gold_v2/*"
    ]
  }
}

resource "aws_iam_role_policy" "glue_crawler_s3_read" {
  name   = "StrataFlowGlueGoldReadOnly"
  role   = aws_iam_role.strataflow_glue_crawler.id
  policy = data.aws_iam_policy_document.glue_crawler_s3_read.json
}

resource "aws_glue_crawler" "products_current" {
  name          = "strataflow-gold-products-crawler"
  database_name = aws_glue_catalog_database.strataflow_catalog.name
  role          = aws_iam_role.strataflow_glue_crawler.arn
  description   = "Discovers the StrataFlow Gold current products dataset"

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/gold_v2/products_current/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy.glue_crawler_s3_read
  ]
}

resource "aws_glue_crawler" "category_metrics" {
  name          = "strataflow-gold-category-crawler"
  database_name = aws_glue_catalog_database.strataflow_catalog.name
  role          = aws_iam_role.strataflow_glue_crawler.arn
  description   = "Discovers the StrataFlow Gold category inventory metrics dataset"

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/gold_v2/category_inventory_metrics/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy.glue_crawler_s3_read
  ]
}

# Existing dbt workgroup â€” preserve its current configuration
resource "aws_athena_workgroup" "strataflow_dbt" {
  name        = "strataflow-dbt"
  description = "Dedicated workgroup for StrataFlow dbt transformations"
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true
  }
}

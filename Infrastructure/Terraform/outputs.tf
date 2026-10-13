output "data_lake_bucket" {
  value = aws_s3_bucket.data_lake.bucket
}

output "data_lake_arn" {
  value = aws_s3_bucket.data_lake.arn
}
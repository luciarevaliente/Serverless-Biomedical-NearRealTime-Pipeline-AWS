# Kinesis Data Stream for biomedical data ingestion
resource "aws_kinesis_stream" "biomedical_stream" {
  name             = "${var.project_name}-${var.environment}-stream"
  # shard_count      = 5 #1 --> 2 --> 5
  retention_period = 24

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
resource "aws_s3_bucket" "datalake" {
  bucket = local.bucket_name

  tags = merge(var.tags, {
    Name = local.bucket_name
  })
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "bronze-transition-to-ia"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = var.bronze_transition_ia_days
      storage_class = "STANDARD_IA"
    }

    dynamic "expiration" {
      for_each = var.enable_bronze_expiration ? [1] : []
      content {
        days = var.bronze_expiration_days
      }
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_object" "bronze_placeholder" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "${var.bronze_prefix}/.keep"
  content = ""
}

resource "aws_s3_object" "silver_placeholder" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "${var.silver_prefix}/.keep"
  content = ""
}

resource "aws_s3_object" "gold_placeholder" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "${var.gold_prefix}/.keep"
  content = ""
}

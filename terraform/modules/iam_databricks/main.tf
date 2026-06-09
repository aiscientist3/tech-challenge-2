# IAM User — programmatic access for Databricks Serverless (Secret Scope + deltalake)
resource "aws_iam_user" "databricks_s3" {
  name = "${local.name_prefix}-databricks-s3"
  path = "/"

  tags = merge(var.tags, {
    Name        = "${local.name_prefix}-databricks-s3"
    Description = "Programmatic S3 access for Databricks Serverless batch ingestion"
  })
}

resource "aws_iam_user_policy" "databricks_s3" {
  name   = "${local.name_prefix}-databricks-s3-policy"
  user   = aws_iam_user.databricks_s3.name
  policy = local.s3_policy_document
}

resource "aws_iam_access_key" "databricks_s3" {
  user = aws_iam_user.databricks_s3.name
}

# IAM Role + Instance Profile — classic Databricks clusters / future streaming
resource "aws_iam_role" "databricks_s3" {
  name = "${local.name_prefix}-databricks-s3-role"
  path = "/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EC2AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })

  tags = merge(var.tags, {
    Name        = "${local.name_prefix}-databricks-s3-role"
    Description = "Instance profile role for Databricks classic compute S3 access"
  })
}

resource "aws_iam_role_policy" "databricks_s3" {
  name   = "${local.name_prefix}-databricks-s3-role-policy"
  role   = aws_iam_role.databricks_s3.id
  policy = local.s3_policy_document
}

resource "aws_iam_instance_profile" "databricks_s3" {
  name = "${local.name_prefix}-databricks-s3-profile"
  role = aws_iam_role.databricks_s3.name

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-databricks-s3-profile"
  })
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

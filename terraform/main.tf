module "s3_datalake" {
  source = "./modules/s3_datalake"

  project_name              = var.project_name
  environment               = var.environment
  aws_region                = var.aws_region
  bucket_name               = var.bucket_name
  bronze_prefix             = var.bronze_prefix
  silver_prefix             = var.silver_prefix
  gold_prefix               = var.gold_prefix
  bronze_transition_ia_days = var.bronze_transition_ia_days
  enable_bronze_expiration  = var.enable_bronze_expiration
  bronze_expiration_days    = var.bronze_expiration_days
  tags                      = local.common_tags
}

module "iam_databricks" {
  source = "./modules/iam_databricks"

  project_name = var.project_name
  environment  = var.environment
  bucket_arn   = module.s3_datalake.bucket_arn
  bucket_name  = module.s3_datalake.bucket_name
  tags         = local.common_tags
}

module "databricks_job" {
  source = "./modules/databricks_job"

  job_name                  = var.job_name
  git_repo_url              = var.git_repo_url
  git_branch                = var.git_branch
  git_provider              = var.git_provider
  job_python_file           = var.job_python_file
  job_parameters            = var.job_parameters
  job_timeout_seconds       = var.job_timeout_seconds
  job_pypi_dependencies     = var.job_pypi_dependencies
  enable_job_schedule       = var.enable_job_schedule
  job_schedule_cron         = var.job_schedule_cron
  register_instance_profile = var.register_instance_profile
  instance_profile_arn      = module.iam_databricks.instance_profile_arn

  providers = {
    databricks = databricks
  }

  depends_on = [module.iam_databricks]
}

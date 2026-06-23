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

  project_name              = var.project_name
  environment               = var.environment
  bucket_arn                = module.s3_datalake.bucket_arn
  bucket_name               = module.s3_datalake.bucket_name
  enable_cloudwatch_metrics = var.enable_monitoring
  tags                      = local.common_tags
}

module "monitoring" {
  count  = var.enable_monitoring ? 1 : 0
  source = "./modules/monitoring"

  project_name               = var.project_name
  environment                = var.environment
  aws_region                 = var.aws_region
  bucket_name                = module.s3_datalake.bucket_name
  alert_email                = var.alert_email
  metric_namespace           = var.metric_namespace
  job_duration_alarm_seconds = var.job_duration_alarm_seconds
  enable_s3_error_alarm      = var.enable_s3_error_alarm
  tags                       = local.common_tags
}

module "databricks_job" {
  source = "./modules/databricks_job"

  job_name                     = var.job_name
  job_task_key                 = "bronze_batch_ingestion"
  git_repo_url                 = var.git_repo_url
  git_branch                   = var.git_branch
  git_provider                 = var.git_provider
  job_python_file              = var.job_python_file
  job_parameters               = var.job_parameters
  job_timeout_seconds          = var.job_timeout_seconds
  job_pypi_dependencies        = var.job_pypi_dependencies
  job_environment_version      = var.job_environment_version
  enable_job_schedule          = var.enable_job_schedule
  job_schedule_cron            = var.job_schedule_cron
  register_instance_profile    = var.register_instance_profile
  instance_profile_arn         = module.iam_databricks.instance_profile_arn
  alert_emails                 = var.enable_monitoring && var.alert_email != "" ? [var.alert_email] : []
  alert_on_success             = var.alert_on_success
  job_duration_warning_seconds = var.job_duration_warning_seconds

  providers = {
    databricks = databricks
  }

  depends_on = [module.iam_databricks]
}

module "databricks_job_streaming" {
  count  = var.enable_streaming_job ? 1 : 0
  source = "./modules/databricks_job"

  job_name                     = var.streaming_job_name
  job_task_key                 = "bronze_streaming_ingestion"
  git_repo_url                 = var.git_repo_url
  git_branch                   = var.git_branch
  git_provider                 = var.git_provider
  job_python_file              = var.streaming_job_python_file
  job_parameters               = var.streaming_job_parameters
  job_timeout_seconds          = var.streaming_job_timeout_seconds
  job_pypi_dependencies        = var.streaming_job_pypi_dependencies
  job_environment_version      = var.job_environment_version
  enable_job_schedule          = var.enable_streaming_job_schedule
  job_schedule_cron            = var.streaming_job_schedule_cron
  register_instance_profile    = false
  instance_profile_arn         = module.iam_databricks.instance_profile_arn
  alert_emails                 = var.enable_monitoring && var.alert_email != "" ? [var.alert_email] : []
  alert_on_success             = var.alert_on_success
  job_duration_warning_seconds = var.job_duration_warning_seconds

  providers = {
    databricks = databricks
  }

  depends_on = [module.iam_databricks]
}

module "databricks_job_silver" {
  count  = var.enable_silver_job ? 1 : 0
  source = "./modules/databricks_job"

  job_name                     = var.silver_job_name
  job_task_key                 = "silver_batch_ingestion"
  git_repo_url                 = var.git_repo_url
  git_branch                   = var.git_branch
  git_provider                 = var.git_provider
  job_python_file              = var.silver_job_python_file
  job_parameters               = var.silver_job_parameters
  job_timeout_seconds          = var.silver_job_timeout_seconds
  job_pypi_dependencies        = var.silver_job_pypi_dependencies
  job_environment_version      = var.job_environment_version
  enable_job_schedule          = var.enable_silver_job_schedule
  job_schedule_cron            = var.silver_job_schedule_cron
  register_instance_profile    = false
  instance_profile_arn         = module.iam_databricks.instance_profile_arn
  alert_emails                 = var.enable_monitoring && var.alert_email != "" ? [var.alert_email] : []
  alert_on_success             = var.alert_on_success
  job_duration_warning_seconds = var.job_duration_warning_seconds

  providers = {
    databricks = databricks
  }

  depends_on = [module.iam_databricks]
}

module "databricks_job_gold" {
  count  = var.enable_gold_job ? 1 : 0
  source = "./modules/databricks_job"

  job_name                     = var.gold_job_name
  job_task_key                 = "gold_batch_indicators"
  git_repo_url                 = var.git_repo_url
  git_branch                   = var.git_branch
  git_provider                 = var.git_provider
  job_python_file              = var.gold_job_python_file
  job_parameters               = var.gold_job_parameters
  job_timeout_seconds          = var.gold_job_timeout_seconds
  job_pypi_dependencies        = var.gold_job_pypi_dependencies
  job_environment_version      = var.job_environment_version
  enable_job_schedule          = var.enable_gold_job_schedule
  job_schedule_cron            = var.gold_job_schedule_cron
  register_instance_profile    = false
  instance_profile_arn         = module.iam_databricks.instance_profile_arn
  alert_emails                 = var.enable_monitoring && var.alert_email != "" ? [var.alert_email] : []
  alert_on_success             = var.alert_on_success
  job_duration_warning_seconds = var.job_duration_warning_seconds

  providers = {
    databricks = databricks
  }

  depends_on = [module.iam_databricks]
}

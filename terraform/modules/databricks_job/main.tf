resource "databricks_instance_profile" "this" {
  count = var.register_instance_profile ? 1 : 0

  instance_profile_arn = var.instance_profile_arn
  skip_validation      = true

  lifecycle {
    precondition {
      condition = (
        !var.register_instance_profile
        || (
          length(var.instance_profile_arn) >= 20
          && startswith(var.instance_profile_arn, "arn:aws:iam::")
        )
      )
      error_message = "instance_profile_arn must be a valid AWS IAM ARN when register_instance_profile is true."
    }
  }
}

resource "databricks_job" "bronze_batch" {
  name = var.job_name

  git_source {
    url      = var.git_repo_url
    provider = var.git_provider
    branch   = var.git_branch
  }

  environment {
    environment_key = local.environment_key

    spec {
      environment_version = var.job_environment_version
      dependencies        = var.job_pypi_dependencies
    }
  }

  task {
    task_key        = "bronze_batch_ingestion"
    environment_key = local.environment_key
    timeout_seconds = var.job_timeout_seconds

    spark_python_task {
      python_file = var.job_python_file
      source      = "GIT"
      parameters  = var.job_parameters
    }
  }

  dynamic "schedule" {
    for_each = var.enable_job_schedule ? [1] : []
    content {
      quartz_cron_expression = var.job_schedule_cron
      timezone_id            = "America/Sao_Paulo"
    }
  }

  max_concurrent_runs = 1
}

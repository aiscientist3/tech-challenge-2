check "monitoring_email_required" {
  assert {
    condition     = !var.enable_monitoring || var.alert_email != ""
    error_message = "Set alert_email in terraform.tfvars when enable_monitoring is true."
  }
}

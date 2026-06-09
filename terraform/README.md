# Terraform — AWS + Databricks Job

Infrastructure as Code for the literacy data pipeline:

- **S3 datalake** with encryption, versioning, lifecycle rules, and medallion layout placeholders
- **IAM dual access**: programmatic user (Serverless + Secret Scope) and instance profile (classic compute)
- **Databricks Job** for Bronze batch ingestion from Git

MSK and remote Terraform state backend are out of scope for this module set.

## Prerequisites

| Tool | Purpose |
|---|---|
| [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5 | Apply infrastructure |
| [AWS CLI](https://aws.amazon.com/cli/) | Authenticate to AWS |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/) | Sync secrets after apply |
| Databricks PAT | `TF_VAR_databricks_token` for the provider |
| Git repo connected in Databricks | Job pulls `ingestion/batch/main.py` from GitHub |

Ensure your AWS credentials can create S3 buckets and IAM users/roles in the target account/region.

## Quick start

```bash
cd terraform

# 1. Configure variables
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars (databricks_host, git_repo_url, etc.)

# 2. AWS profile (personal account example)
export AWS_PROFILE=tech-challenge
export AWS_REGION=us-east-1
aws sts get-caller-identity   # confirm Account ID

# 3. Export sensitive token (never commit)
export TF_VAR_databricks_token="dapi..."

# 4. Initialise and review
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan

# 5. Apply
terraform apply
```

## Module layout

```
terraform/
├── main.tf                 # Root wiring
├── variables.tf
├── outputs.tf
├── providers.tf
├── versions.tf
├── terraform.tfvars.example
└── modules/
    ├── s3_datalake/        # Bucket, lifecycle, medallion placeholders
    ├── iam_databricks/     # IAM user + keys, role + instance profile
    └── databricks_job/     # Serverless Git job + instance profile registration
```

## Post-apply: sync Databricks secrets

Terraform creates AWS resources and outputs credentials. **Secrets are not written into Databricks by Terraform** (avoids storing credentials in Terraform state beyond sensitive outputs).

### AWS Secret Scope (`aws`)

```bash
cd terraform

databricks secrets create-scope aws 2>/dev/null || true

databricks secrets put --scope aws --key s3-bucket \
  --string-value "$(terraform output -raw s3_bucket_name)"

databricks secrets put --scope aws --key access-key-id \
  --string-value "$(terraform output -raw access_key_id)"

databricks secrets put --scope aws --key secret-access-key \
  --string-value "$(terraform output -raw secret_access_key)"
```

These keys match the pipeline defaults in `ingestion/batch/config.py`:
- `aws/s3-bucket`
- `aws/access-key-id`
- `aws/secret-access-key`

### GCP Secret Scope (`gcp`) — manual

GCP service account JSON and project ID remain outside Terraform:

```bash
databricks secrets create-scope gcp 2>/dev/null || true

databricks secrets put --scope gcp --key project-id \
  --string-value "YOUR_GCP_PROJECT_ID"

databricks secrets put --scope gcp --key service-account-json \
  --string-value "$(cat /path/to/service-account.json)"
```

### Verify

1. Open the job URL from `terraform output databricks_job_url`
2. Run the job manually
3. Confirm Delta tables under `s3://<bucket>/bronze/br_inep_alfabetizacao/`

## Variables reference

| Variable | Default | Description |
|---|---|---|
| `project_name` | `tech-challenge-2` | Resource name prefix |
| `environment` | `dev` | Environment suffix |
| `aws_region` | `us-east-1` | AWS region |
| `bucket_name` | *(derived)* | Optional explicit S3 bucket name |
| `databricks_host` | — | Workspace URL (required) |
| `databricks_token` | — | PAT via `TF_VAR_databricks_token` (required) |
| `git_repo_url` | — | GitHub repo URL (required) |
| `git_branch` | `main` | Branch for the job |
| `job_parameters` | `["--sources","all","--years","2023,2024"]` | CLI args for `main.py` |
| `enable_job_schedule` | `false` | Enable cron schedule |
| `register_instance_profile` | `true` | Register IAM instance profile in Databricks |

See [`terraform.tfvars.example`](terraform.tfvars.example) for a full template.

## Outputs

| Output | Description |
|---|---|
| `s3_bucket_name` | Datalake bucket |
| `access_key_id` | IAM user key (sensitive) |
| `secret_access_key` | IAM user secret (sensitive) |
| `instance_profile_arn` | For classic Databricks clusters |
| `databricks_job_id` | Bronze ingestion job ID |
| `databricks_job_url` | Link to job in workspace |
| `secrets_sync_commands` | Hint commands for secret sync |

## Remote state (optional)

This project uses **local state** by default. For team use, bootstrap a separate state bucket and DynamoDB lock table, then add:

```hcl
terraform {
  backend "s3" {
    bucket         = "your-terraform-state-bucket"
    key            = "tech-challenge-2/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}
```

## Destroy

```bash
terraform destroy
```

Warning: destroys the S3 bucket (must be empty or force-destroy enabled). Empty Bronze/Silver/Gold data first if needed.

## Troubleshooting

| Issue | Action |
|---|---|
| Bucket name already taken | Set `bucket_name` to a globally unique value in `terraform.tfvars` |
| Databricks job cannot pull Git | Connect GitHub in Databricks **Settings → Linked accounts → Git integration** |
| `dummy-arn` on instance profile | Set `register_instance_profile = false` (default for Serverless). Run full `terraform apply` without `-target`. |
| Instance profile registration fails | Requires workspace admin + Databricks trust policy on IAM role; keep `register_instance_profile = false` for Serverless |
| `Client-1 channel for REPL` | Use `job_environment_version = "2"` (default). Do not use deprecated `client = "1"`. |
| Job fails on packages | Adjust `job_pypi_dependencies` in `terraform.tfvars` |

variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "clickhouse_cluster_size" {
  description = "ClickHouse cluster size"
  type        = string
  default     = "small"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default = {
    Project   = "Track Platform"
    ManagedBy = "Terraform"
  }
}

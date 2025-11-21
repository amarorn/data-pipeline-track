# ============================================
# Track Platform - Infrastructure as Code
# ============================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "track-terraform-state"
    key    = "platform/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# ============================================
# S3 Buckets para Data Lake
# ============================================

resource "aws_s3_bucket" "datalake" {
  bucket = "track-datalake-${var.environment}"

  tags = {
    Name        = "Track Data Lake"
    Environment = var.environment
    ManagedBy   = "Terraform"
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

  # Bronze: mantém por 90 dias
  rule {
    id     = "bronze-lifecycle"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    expiration {
      days = 90
    }
  }

  # Silver: mantém por 365 dias
  rule {
    id     = "silver-lifecycle"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    expiration {
      days = 365
    }
  }

  # Gold: mantém permanentemente (sem expiration)
  rule {
    id     = "gold-lifecycle"
    status = "Enabled"

    filter {
      prefix = "gold/"
    }
  }
}

# ============================================
# IAM User para acesso S3
# ============================================

resource "aws_iam_user" "pipeline_user" {
  name = "track-pipeline-${var.environment}"

  tags = {
    Purpose = "Data Pipeline Access"
  }
}

resource "aws_iam_user_policy" "pipeline_s3_access" {
  name = "s3-datalake-access"
  user = aws_iam_user.pipeline_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_access_key" "pipeline_user" {
  user = aws_iam_user.pipeline_user.name
}

# ============================================
# Outputs
# ============================================

output "s3_bucket_name" {
  value = aws_s3_bucket.datalake.id
}

output "pipeline_user_access_key" {
  value     = aws_iam_access_key.pipeline_user.id
  sensitive = false
}

output "pipeline_user_secret_key" {
  value     = aws_iam_access_key.pipeline_user.secret
  sensitive = true
}

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "zombi" {
  bucket        = var.s3_bucket_name
  force_destroy = true  # Enable instant cleanup on terraform destroy
}

# Note: S3 versioning disabled for test bucket to enable fast cleanup
# For production, re-enable versioning for data protection

resource "aws_iam_role" "zombi_ec2" {
  name               = "zombi-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "zombi_s3" {
  name   = "zombi-s3-policy"
  role   = aws_iam_role.zombi_ec2.id
  policy = data.aws_iam_policy_document.s3_access.json
}

data "aws_iam_policy_document" "s3_access" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      aws_s3_bucket.zombi.arn,
      "${aws_s3_bucket.zombi.arn}/*"
    ]
  }
}

resource "aws_iam_instance_profile" "zombi" {
  name = "zombi-ec2-profile"
  role = aws_iam_role.zombi_ec2.name
}

resource "aws_security_group" "zombi" {
  name        = "zombi-sg"
  description = "Zombi HTTP + SSH"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.http_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "zombi" {
  key_name   = var.key_pair_name
  public_key = file(var.public_key_path)
}

resource "aws_instance" "zombi" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.zombi.key_name
  vpc_security_group_ids = [aws_security_group.zombi.id]
  iam_instance_profile   = aws_iam_instance_profile.zombi.name

  user_data = templatefile("${path.module}/user_data.sh", {
    region        = var.aws_region
    s3_bucket     = aws_s3_bucket.zombi.bucket
    zombi_image   = var.zombi_image
  })

  tags = {
    Name = "zombi-ec2"
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

output "instance_public_ip" {
  value = aws_instance.zombi.public_ip
}

output "s3_bucket" {
  value = aws_s3_bucket.zombi.bucket
}

output "ssh_command" {
  value = "ssh -i ~/.ssh/id_ed25519 ubuntu@${aws_instance.zombi.public_ip}"
}

output "health_url" {
  value = "http://${aws_instance.zombi.public_ip}:8080/health"
}

output "stats_url" {
  value = "http://${aws_instance.zombi.public_ip}:8080/stats"
}

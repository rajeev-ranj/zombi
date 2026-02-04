variable "aws_region" {
  type    = string
  default = "ap-southeast-1"
}

variable "s3_bucket_name" {
  type = string
}

variable "instance_type" {
  type    = string
  default = "t3.micro"
}

variable "enable_loadgen" {
  type    = bool
  default = false
}

variable "loadgen_instance_type" {
  type    = string
  default = "t3.small"
}

variable "key_pair_name" {
  type = string
}

variable "public_key_path" {
  type = string
}

variable "ssh_cidr" {
  type    = string
  default = "0.0.0.0/0"
}

variable "http_cidr" {
  type    = string
  default = "0.0.0.0/0"
}

variable "zombi_image" {
  type    = string
  default = "ghcr.io/rajeev-ranj/zombi:latest"
}

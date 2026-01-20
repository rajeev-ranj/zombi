#!/usr/bin/env bash
set -euo pipefail

REGION=${1:-ap-southeast-1}

# Install CloudWatch agent (Ubuntu)
apt-get update
apt-get install -y amazon-cloudwatch-agent

# Copy config
install -m 0644 /opt/zombi/cloudwatch-agent.json /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

# Start agent
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
  -s

echo "CloudWatch agent started in region ${REGION}"

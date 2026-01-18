#!/bin/bash
set -e

# Zombi Provisioning Script
# Usage: ./provision.sh

echo ">>> Updating system packages..."
sudo apt-get update && sudo apt-get upgrade -y

echo ">>> Installing dependencies..."
sudo apt-get install -y curl git ufw

if ! command -v docker &> /dev/null; then
    echo ">>> Installing Docker..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    sudo usermod -aG docker $USER
    rm get-docker.sh
else
    echo ">>> Docker already installed."
fi

echo ">>> Configuring Firewall (UFW)..."
sudo ufw allow 22/tcp
sudo ufw allow 8080/tcp
sudo ufw --force enable

echo ">>> Creating data directories..."
sudo mkdir -p /var/lib/zombi
sudo chown -R 1000:1000 /var/lib/zombi 2>/dev/null || true

echo ">>> Provisioning complete! Please log out and back in for Docker group changes to take effect."

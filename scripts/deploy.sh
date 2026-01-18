#!/bin/bash
set -e

# Zombi Deployment Script
# Usage: ./deploy.sh

echo ">>> Pulling latest image..."
docker-compose pull

echo ">>> Restarting services..."
docker-compose up -d

echo ">>> Pruning old images..."
docker image prune -f

echo ">>> Deployment complete. Zombi is running."
echo ">>> Check logs with: docker-compose logs -f"

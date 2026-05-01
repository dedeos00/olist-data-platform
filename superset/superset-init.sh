#!/bin/bash
set -e

echo "Upgrading Superset metadata database..."
superset db upgrade

echo "Creating admin user..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin || true

echo "Initializing Superset roles and permissions..."
superset init

echo "Superset initialization completed."

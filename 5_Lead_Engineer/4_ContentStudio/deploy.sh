#!/bin/bash
# Content Studio v8.5 - Deployment Helper
# Usage: ./deploy.sh

set -e

echo "=========================================="
echo "Content Studio v8.5 - Deployment"
echo "=========================================="
echo ""

APP_NAME="content-studio"
SOURCE_PATH="/Workspace/Shared/dev/SkillAIPath/5_Growth_Accelerator/4_ContentStudio"

echo "Step 1: Checking if app exists..."
if databricks apps get "$APP_NAME" &>/dev/null; then
    echo "   ⚠️  App exists - stopping and deleting old version..."
    databricks apps stop "$APP_NAME" 2>/dev/null || true
    sleep 2
    databricks apps delete "$APP_NAME" 2>/dev/null || true
    sleep 2
fi

echo ""
echo "Step 2: Creating app..."
databricks apps create "$APP_NAME" \
    --description "Multi-Model Content Review Platform v8.5"

echo ""
echo "Step 3: Deploying source code..."
databricks apps deploy "$APP_NAME" \
    --source-code-path "$SOURCE_PATH"

echo ""
echo "Step 4: Starting app..."
sleep 5
databricks apps start "$APP_NAME"

echo ""
echo "Step 5: Waiting for startup (30 seconds)..."
sleep 30

echo ""
echo "=========================================="
echo "✅ DEPLOYMENT COMPLETE"
echo "=========================================="
echo ""

echo "Getting app URL..."
databricks apps get "$APP_NAME"

echo ""
echo "App should be running at the URL above!"

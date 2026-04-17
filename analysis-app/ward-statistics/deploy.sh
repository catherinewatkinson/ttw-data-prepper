#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPORT="$SCRIPT_DIR/report.html"

# Pre-flight checks
command -v wrangler >/dev/null 2>&1 || { echo "ERROR: wrangler not installed. Run: npm install -g wrangler"; exit 1; }

if [ ! -f "$REPORT" ]; then
    echo "ERROR: report.html not found. Run generate_report.py first."
    exit 1
fi

DEPLOY_DIR=$(mktemp -d)
trap 'rm -rf "$DEPLOY_DIR"' EXIT

cp "$REPORT" "$DEPLOY_DIR/index.html"

echo "Deploying to Cloudflare Pages..."
wrangler pages deploy "$DEPLOY_DIR" --project-name brent-greens-stats

echo "Done. Visit: https://brent-greens-stats.pages.dev"

#!/bin/bash
# setup_github_actions.sh
# Setup GitHub Actions for the project

echo "======================================"
echo "GitHub Actions Setup"
echo "======================================"
echo ""

# Create directory structure
mkdir -p .github/workflows
mkdir -p data

# Create the workflow file (copy from artifact above)
echo "Creating workflow file..."

cat > .github/workflows/api-pipeline.yml << 'EOF'
# Paste the workflow content from the artifact above
EOF

echo "Created .github/workflows/api-pipeline.yml"
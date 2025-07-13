#!/bin/bash

# Databricks Asset Bundle Cleanup Script
# This script removes your deployed DABs project

set -e  # Exit on any error

echo "🧹 Databricks Asset Bundle Cleanup"
echo "=================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    print_error "Databricks CLI is not installed or not in PATH"
    exit 1
fi

# Check if Terraform is installed
if ! command -v terraform &> /dev/null; then
    print_error "Terraform is not installed or not in PATH"
    echo "Please install it with: brew install terraform"
    exit 1
fi

print_success "Terraform found"

# Show what will be destroyed
print_status "Getting current deployment summary..."
databricks bundle summary

echo ""
print_warning "⚠️  This will permanently delete all deployed resources!"
print_warning "   • Jobs will be deleted"
print_warning "   • Notebooks will be removed from workspace"
print_warning "   • All bundle artifacts will be cleaned up"
echo ""

read -p "Are you sure you want to destroy the bundle? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Destroying bundle..."
    if databricks bundle destroy; then
        print_success "Bundle destroyed successfully!"
    else
        print_error "Bundle destruction failed"
        exit 1
    fi
else
    print_status "Cleanup cancelled"
    exit 0
fi

print_status "Getting Terraform resources..."
cd terraform
terraform destroy
if [ $? -ne 0 ]; then
    print_error "Terraform destroy failed"
    exit 1
fi

cd ..
print_success "Terraform resources destroyed"

echo ""
print_success "Cleanup completed! 🎉" 
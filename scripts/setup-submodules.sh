#!/bin/bash

# Go-RocketMQ Submodule Setup Script
# This script initializes and updates Git submodules for the Go-RocketMQ project

set -e  # Exit on any error

echo "Initializing Go-RocketMQ submodules..."

# Initialize submodules
git submodule init

# Update submodules to the latest commit
git submodule update --remote

echo "Submodules initialized and updated successfully!"

# List current submodule status
echo ""
echo "Current submodule status:"
git submodule status

echo ""
echo "Setup complete! You can now build and test the project."
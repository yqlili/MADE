#!/bin/bash

# Set the target directory to store data to an environment variable
export TARGET_DIR=$(dirname "$(pwd)")

echo "Target working directory set to: $TARGET_DIR"

python3 pipeline.py

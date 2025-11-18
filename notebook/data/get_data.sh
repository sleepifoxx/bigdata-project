#!/bin/bash

# Google Drive file ID and output zip
FILEID="1rsXQVwAYy6OWece4LfcJh0C6-ROkoaA3"
OUT="data.zip"

echo "Checking if gdown is installed..."
if ! command -v gdown &> /dev/null
then
    echo "gdown not found. Installing..."
    pip install gdown
    if [ $? -ne 0 ]; then
        echo "Error: Failed to install gdown. Please install it manually."
        exit 1
    fi
else
    echo "gdown is already installed."
fi

echo "Downloading data from Google Drive..."
gdown --id $FILEID -O $OUT

# Check if download succeeded
if [ $? -eq 0 ]; then
    echo "Download completed successfully."
    
    # Extract zip
    echo "Extracting ${OUT}..."
    unzip -o $OUT
    
    if [ $? -eq 0 ]; then
        echo "Extraction completed successfully."
        # Remove zip
        rm $OUT
        echo "Cleanup completed."
    else
        echo "Error: Failed to extract ${OUT}"
        exit 1
    fi
else
    echo "Error: Failed to download the file"
    exit 1
fi

echo "Data download and extraction process completed!"

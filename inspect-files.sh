#!/bin/bash

# Create temp directory for downloaded files
mkdir -p tmp/downloaded-files
cd tmp/downloaded-files

echo "📥 Downloading files from S3..."

# Download all files
aws --endpoint-url=http://localhost:4566 s3 sync s3://slatedb-test . --quiet

echo "📁 Local file structure:"
find . -type f -exec ls -lh {} \;

echo ""
echo "📝 WAL file contents:"
for file in wal/*; do
    if [ -f "$file" ]; then
        echo "File: $file"
        echo "Size: $(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null) bytes"
        echo "Hex dump (first 200 bytes):"
        hexdump -C "$file" | head -10
        echo "---"
    fi
done

echo ""
echo "🗃️ SST file contents:"
for file in data/l0/*.data; do
    if [ -f "$file" ]; then
        echo "File: $file"
        echo "Size: $(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null) bytes"
        echo "Hex dump (first 200 bytes):"
        hexdump -C "$file" | head -10
        echo "---"
    fi
done

cd ../..
#!/bin/bash

echo "🗂️ Checking S3 files in LocalStack..."

# List all objects in the bucket
echo "📋 All files in slatedb-test bucket:"
aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb-test --recursive

echo ""
echo "📝 WAL files:"
aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb-test/wal/ --recursive

echo ""
echo "🗃️ Data files:"
aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb-test/data/ --recursive

echo ""
echo "📊 File sizes:"
aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb-test --recursive --human-readable --summarize
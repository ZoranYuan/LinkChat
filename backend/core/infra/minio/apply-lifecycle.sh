#!/usr/bin/env bash
# 给 bucket 配置"中断的分片上传 3 天后自动 Abort"（S3/MinIO 通用标准）。
# 应用层不用写任何 Abort 逻辑，存储层自动回收半成品分片，避免持续计费和孤儿。
#
# 注意：只加 AbortIncompleteMultipartUpload 规则，绝对不要加 Expiration——Expiration 会删除包括已完成文件在内的所有对象。
set -euo pipefail

ALIAS="${MINIO_ALIAS:-local}"
BUCKET="${MINIO_BUCKET:-imchat}"
PREFIX="${MINIO_PREFIX:-minio/direct/}"

mc alias set "$ALIAS" "${MINIO_ENDPOINT:-http://localhost:9000}" \
  "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}" >/dev/null

mc ilm rule add "$ALIAS/$BUCKET" \
  --prefix "$PREFIX" \
  --abort-incomplete-multipart-upload \
  --days 3 \
  --id "abort-incomplete-multipart-direct"

echo "lifecycle rule applied"
mc ilm rule list "$ALIAS/$BUCKET"

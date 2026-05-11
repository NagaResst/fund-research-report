#!/bin/bash
# 基金研究平台启动脚本
# 用法: ./start.sh

set -e
cd "$(dirname "$0")"

echo "📦 检查 Python 依赖..."
pip3 install -q -r requirements.txt

echo "🚀 启动服务..."
python3 server.py

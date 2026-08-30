#!/bin/sh
# 启动德州扑克应用（3017端口）
export PORT=3017
export DATA_FILE=/mnt/mmc1-4/poker-app/data/state.json
cd /mnt/mmc1-4/poker-app
node server/server.js > /mnt/mmc1-4/poker-app/app.log 2>&1 &
echo $! > /mnt/mmc1-4/poker-app/app.pid
sleep 2
# 启动 HTTPS 代理（8443端口）
node https-proxy.js > /mnt/mmc1-4/poker-app/proxy.log 2>&1 &
echo $! > /mnt/mmc1-4/poker-app/proxy.pid
echo "德州扑克应用已启动: HTTP :3017, HTTPS :8443"

#!/usr/bin/env bash
###############################################################################
# 用法: ./log.sh 2025-07-14      # 指定日期
#       ./log.sh                 # 默认今天
###############################################################################

set -e                                    # 任一命令出错即退出
#切换java 版本到1.8
# export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home
# export PATH=$JAVA_HOME/bin:$PATH    # 把 1.8 放到 PATH 最前
# hash -r                             # 清命令缓存（zsh/bash）
# java -version                       # 应显示 1.8.0_xxx

BASE_DIR=/opt/module/applog               # JAR 与 application.yml 所在目录
JAR=gmall2020-mock-log-2021-11-29.jar

cd "$BASE_DIR" || { echo "目录不存在: $BASE_DIR"; exit 1; }

# 1. 解析日期参数
DATE=${1:-$(date +%F)}
echo "[Mock] 生成日期: $DATE"

# 2. 更新 application.yml 的 mock.date
APP_YML=application.yml
if [[ "$OSTYPE" == "darwin"* ]]; then      # macOS
  sed -i "" "s/^mock\.date:.*/mock.date: \"$DATE\"/" "$APP_YML"
else                                       # Linux
  sed -i "s/^mock\.date:.*/mock.date: \"$DATE\"/" "$APP_YML"
fi
echo "[Mock] application.yml 已更新"

# 3. 确保日志目录
mkdir -p "$BASE_DIR/log"

# 4. 启动 JAR（后台），输出到 log/mock_<DATE>.out
nohup java -jar "$JAR" > "$BASE_DIR/log/mock_${DATE}.out" 2>&1 &
PID=$!
echo "[Mock] JAR 已后台启动 (PID=$PID)，日志: $BASE_DIR/log/mock_${DATE}.out"

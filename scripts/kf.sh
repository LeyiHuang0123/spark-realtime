#!/usr/bin/env bash
###############################################################################
#  用法： ./kf.sh start          # 启动 Kafka（后台）
#        ./kf.sh stop           # 停止 Kafka
#        ./kf.sh list           # 列出现有 topic
#        ./kf.sh kc ods_log     # Console Consumer
#        ./kf.sh kp ods_log     # Console Producer
#        ./kf.sh describe ods_log
#        ./kf.sh delete ods_log
###############################################################################

KAFKA_HOME="$(brew --prefix kafka)"           # Home-brew 前缀
KAFKA_BIN="$KAFKA_HOME/libexec/bin"           # 真实脚本目录
CONFIG="$KAFKA_HOME/config/server.properties" # server.properties 路径
BROKERS="localhost:9092"

# 强制脚本用 JDK 17 运行 Kafka CLI
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH=$JAVA_HOME/bin:$PATH

usage() {
  echo "Usage: kf.sh {start|stop|list|kc topic|kp topic|describe topic|delete topic}"
  exit 1
}

[[ $# -lt 1 ]] && usage
CMD=$1
TOPIC=$2

case $CMD in
# ---------------------------------------------------------------------------#
start)
  echo "==> START Kafka (Mac 本地)"
  "$KAFKA_BIN/kafka-server-start.sh" -daemon "$CONFIG"
  ;;

stop)
  echo "==> STOP Kafka"
  "$KAFKA_BIN/kafka-server-stop.sh"
  ;;

# ---------------------------------------------------------------------------#
kc)   # Console Consumer
  [[ -z $TOPIC ]] && usage
  "$KAFKA_BIN/kafka-console-consumer.sh" \
      --bootstrap-server "$BROKERS" \
      --topic "$TOPIC" --from-beginning
  ;;

kp)   # Console Producer
  [[ -z $TOPIC ]] && usage
  "$KAFKA_BIN/kafka-console-producer.sh" \
      --broker-list "$BROKERS" \
      --topic "$TOPIC"
  ;;

# ---------------------------------------------------------------------------#
list)
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKERS" --list
  ;;

describe)
  [[ -z $TOPIC ]] && usage
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKERS" \
      --describe --topic "$TOPIC"
  ;;

delete)
  [[ -z $TOPIC ]] && usage
  read -rp "⚠️  确认删除 Topic '$TOPIC'? (y/N) " yn
  [[ $yn == [Yy] ]] || exit 0
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKERS" \
      --delete --topic "$TOPIC"
  ;;

# ---------------------------------------------------------------------------#
*)
  usage
  ;;
esac

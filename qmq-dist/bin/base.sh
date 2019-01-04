#!/usr/bin/env bash
set -euo pipefail

TIMESTAMP=$(date +%s)
QMQ_CFG_DIR="$QMQ_BIN_DIR/../conf"
QMQ_PID_DIR="$QMQ_BIN_DIR/../pid"
QMQ_LOG_DIR="$QMQ_BIN_DIR/../logs"

if [[ ! -w "$QMQ_PID_DIR" ]] ; then
mkdir -p "$QMQ_PID_DIR"
fi

if [[ ! -w "$QMQ_LOG_DIR" ]] ; then
mkdir -p "$QMQ_LOG_DIR"
fi

CLASSPATH="$QMQ_CFG_DIR"
for i in "$QMQ_BIN_DIR"/../lib/*
do
CLASSPATH="$i:$CLASSPATH"
done

#!/usr/bin/env bash
set -euo pipefail

QMQ_BIN="${BASH_SOURCE-$0}"
QMQ_BIN="$(dirname "${QMQ_BIN}")"
QMQ_BIN_DIR="$(cd "${QMQ_BIN}"; pwd)"
QMQ_TOOLS_MAIN="qunar.tc.qmq.tools.Tools"

. "$QMQ_BIN_DIR/base.sh"
. "$QMQ_BIN_DIR/tools-env.sh"

if [[ "$JAVA_HOME" != "" ]]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi

ZOO_CMD=(exec "$JAVA")
"${ZOO_CMD[@]}" -cp "$CLASSPATH" ${JAVA_OPTS} ${QMQ_TOOLS_MAIN} $@

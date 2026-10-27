#!/usr/bin/env bash
# MapReduce demo runner (Linux/macOS) - same flow as run-demo.ps1
set -euo pipefail

# ============================== CONFIG ==============================
CONTAINER=""              # "" = auto-detect (prefers *namenode*)
ROWS=1000000
HDFS_BASE=/demo
REDUCERS=2
CONTAINER_MEM_MB=1024
VARIANTS="a b b-mapjoin"
REGENERATE_DATA=true
# ====================================================================

DIR="$(cd "$(dirname "$0")" && pwd)"
JAR_LOCAL="$DIR/mr-demo.jar"; JAR_REMOTE=/tmp/mr-demo.jar; DATA_TMP=/tmp/mrdemo-data
step() { printf '\n\033[36m=== %s\033[0m\n' "$*"; }
inc()  { docker exec "$CONTAINER" bash -c "$1"; }

step "Locating Hadoop container"
if [ -z "$CONTAINER" ]; then
  CONTAINER=$(docker ps --format '{{.Names}}' | grep -i namenode | head -1 || true)
  [ -z "$CONTAINER" ] && CONTAINER=$(docker ps --format '{{.Names}} {{.Image}}' | grep -i hadoop | head -1 | cut -d' ' -f1 || true)
  [ -z "$CONTAINER" ] && { echo "No Hadoop container found - set CONTAINER"; exit 1; }
fi
HH=$(docker exec "$CONTAINER" bash -c 'echo ${HADOOP_HOME:-$(dirname $(dirname $(readlink -f $(which hadoop))))}' | tail -1)
echo "Container: $CONTAINER   HADOOP_HOME: $HH"

XMX=$((CONTAINER_MEM_MB * 8 / 10))
OPTS="-D yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=$HH -D mapreduce.map.env=HADOOP_MAPRED_HOME=$HH -D mapreduce.reduce.env=HADOOP_MAPRED_HOME=$HH \
-D yarn.app.mapreduce.am.resource.mb=$CONTAINER_MEM_MB -D yarn.app.mapreduce.am.command-opts=-Xmx${XMX}m \
-D mapreduce.map.memory.mb=$CONTAINER_MEM_MB -D mapreduce.map.java.opts=-Xmx${XMX}m \
-D mapreduce.reduce.memory.mb=$CONTAINER_MEM_MB -D mapreduce.reduce.java.opts=-Xmx${XMX}m -D demo.reducers=$REDUCERS"

step "Waiting for HDFS"
inc "hdfs dfsadmin -safemode wait"

step "Copying jar"
docker cp "$JAR_LOCAL" "$CONTAINER:$JAR_REMOTE"

if [ "$REGENERATE_DATA" = true ]; then
  step "Generating $ROWS rows + upload to HDFS"
  inc "java -cp $JAR_REMOTE ch.ffhs.bdn.mr.DataGenerator $DATA_TMP $ROWS"
  inc "hdfs dfs -mkdir -p $HDFS_BASE/input/bigfile $HDFS_BASE/input/bezeichnungen"
  inc "hdfs dfs -put -f $DATA_TMP/bigfile.csv $HDFS_BASE/input/bigfile/"
  inc "hdfs dfs -put -f $DATA_TMP/bezeichnungen.csv $HDFS_BASE/input/bezeichnungen/"
fi

BIG=$HDFS_BASE/input/bigfile; BEZ=$HDFS_BASE/input/bezeichnungen/bezeichnungen.csv
for v in $VARIANTS; do
  case $v in
    a)         ARGS="a $BIG $HDFS_BASE/out/a";                   RES=$HDFS_BASE/out/a/a2_sorted ;;
    b)         ARGS="b $BIG $BEZ $HDFS_BASE/out/b";              RES=$HDFS_BASE/out/b/b3_sorted ;;
    b-mapjoin) ARGS="b-mapjoin $BIG $BEZ $HDFS_BASE/out/b-mapjoin"; RES=$HDFS_BASE/out/b-mapjoin/m2_sorted ;;
    *) echo "unknown variant $v"; continue ;;
  esac
  step "Running '$v'"
  START=$(date +%s)
  inc "hadoop jar $JAR_REMOTE $OPTS $ARGS"
  step "Result '$v' ($(( $(date +%s) - START ))s)"
  inc "hdfs dfs -cat $RES/part-*"
done
echo -e "\nYARN UI: http://localhost:8088   HDFS UI: http://localhost:9870   JobHistory: http://localhost:19888"

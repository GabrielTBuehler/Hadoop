#!/bin/bash
# Sourced inside the Hadoop container before every command (copied to /tmp/mrdemo-env.sh).
# Finds HADOOP_HOME / java even if `docker exec` has a bare environment and no `which`.

# Profile snippets some images use to export Hadoop variables (additive, errors ignored)
for f in /etc/profile.d/*.sh "$HOME/.bashrc"; do
    [ -r "$f" ] && . "$f" >/dev/null 2>&1
done

_mr_is_home() { [ -n "$1" ] && [ -x "$1/bin/hadoop" ]; }

if ! _mr_is_home "$HADOOP_HOME"; then
    HADOOP_HOME=""
    for c in "$HADOOP_MAPRED_HOME" "$HADOOP_PREFIX" "$HADOOP_COMMON_HOME" \
             /opt/hadoop /opt/hadoop-* /usr/local/hadoop /usr/local/hadoop-* \
             /hadoop /usr/lib/hadoop /home/hadoop/hadoop /home/hadoop/hadoop-*; do
        if _mr_is_home "$c"; then HADOOP_HOME="$c"; break; fi
    done
fi

if [ -z "$HADOOP_HOME" ]; then
    p=$(command -v hadoop 2>/dev/null)
    [ -n "$p" ] && HADOOP_HOME=$(cd "$(dirname "$(readlink -f "$p")")/.." && pwd)
fi

if [ -z "$HADOOP_HOME" ]; then
    # last resort: shallow search (find may be missing - then nothing happens)
    p=$(find / -maxdepth 4 -type f -path '*/bin/hadoop' 2>/dev/null | head -1)
    [ -n "$p" ] && HADOOP_HOME=$(cd "$(dirname "$p")/.." && pwd)
fi

export HADOOP_HOME
if [ -n "$HADOOP_HOME" ]; then
    export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
    if [ -z "$HADOOP_CONF_DIR" ]; then
        if [ -d /etc/hadoop ] && [ -f /etc/hadoop/core-site.xml ]; then
            export HADOOP_CONF_DIR=/etc/hadoop
        else
            export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
        fi
    fi
fi

# java (needed for the data generator; hadoop scripts use JAVA_HOME themselves)
if ! command -v java >/dev/null 2>&1; then
    for j in "$JAVA_HOME" /usr/lib/jvm/* /opt/java/* /usr/local/openjdk* /opt/jdk*; do
        if [ -n "$j" ] && [ -x "$j/bin/java" ]; then
            export JAVA_HOME="${JAVA_HOME:-$j}"
            export PATH="$j/bin:$PATH"
            break
        fi
    done
fi

if [ "$1" = "--print" ]; then
    echo "HADOOP_HOME=$HADOOP_HOME"
    echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
    echo "JAVA=$(command -v java 2>/dev/null)"
    fw=$(grep -A1 'mapreduce.framework.name' "$HADOOP_CONF_DIR/mapred-site.xml" 2>/dev/null | grep -o '<value>[^<]*' | sed 's/<value>//')
    echo "FRAMEWORK=${fw:-local (not set)}"
fi

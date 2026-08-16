#!/bin/bash
set -e
mkdir -p /opt/hadoop-apps/classes
HPATH=$(hadoop classpath)
echo "Hadoop classpath: $HPATH"
javac -encoding UTF-8 -classpath "$HPATH" -d /opt/hadoop-apps/classes /host-project/src/mapreduce/JobStatusMapper.java /host-project/src/mapreduce/JobStatusReducer.java /host-project/src/mapreduce/JobStatusDriver.java
echo COMPILE_OK
find /opt/hadoop-apps/classes -name '*.class'
jar -cvf /opt/hadoop-apps/job-status-mr.jar -C /opt/hadoop-apps/classes .
ls -lh /opt/hadoop-apps/job-status-mr.jar
echo BUILD_SUCCESS
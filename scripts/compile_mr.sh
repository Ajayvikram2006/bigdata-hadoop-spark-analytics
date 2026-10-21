#!/bin/bash
set -e
mkdir -p /opt/hadoop-apps/classes
HPATH=$(hadoop classpath)
javac -encoding UTF-8 -classpath $HPATH -d /opt/hadoop-apps/classes /host-project/src/mapreduce/*.java
echo 'Compilation done. Classes:' 
find /opt/hadoop-apps/classes -name '*.class'
jar -cvf /opt/hadoop-apps/job-status-mr.jar -C /opt/hadoop-apps/classes .
ls -lh /opt/hadoop-apps/job-status-mr.jar
echo BUILD_SUCCESS

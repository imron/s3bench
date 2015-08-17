#!/bin/bash

LOG_CONFIG=./log4j2.xml
LOG_FLAGS="-Dlog4j.configurationFile=$LOG_CONFIG"



MIN_HEAP=50g
MAX_HEAP=60g
HEAP_FLAGS="-Xms$MIN_HEAP -Xmx$MAX_HEAP"

GC_LOG="/var/log/s3bench/s3_garbage_collection.log"
GC_FLAGS="-verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogramAfterFullGC -XX:PrintCMSStatistics=2 -Xloggc:$GC_LOG"

java $LOG_FLAGS $HEAP_FLAGS $GC_FLAGS -jar $1

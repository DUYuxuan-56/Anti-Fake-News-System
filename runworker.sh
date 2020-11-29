#!/bin/bash

source settings.sh

JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"

MASTER_PORT="10000"
echo Port number: $KV_PORT

WORKER_PORT=`shuf -i 10000-10999 -n 1`

$JAVA_HOME/bin/java WorkerAA localhost $MASTER_PORT $WORKER_PORT /$USER
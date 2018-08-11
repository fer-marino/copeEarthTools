#!/usr/bin/env bash
JAVA_OPTS="$JAVA_OPTS -Djava.library.path=/usr/local/lib/ -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005";
GDAL_VRT_ENABLE_PYTHON="YES";
#!/bin/bash

if [ ! -f "Dockerfile" ]; then
	echo "Dockerfile not found"
	exit 1
fi

if [ ! -f "target/spring-cloud-stream-module-launcher-1.0.0.BUILD-SNAPSHOT-exec.jar" ]; then
	echo "JAR not available; run mvn package first"
	exit 1
fi

docker build -t 192.168.59.103:5000/module-launcher .

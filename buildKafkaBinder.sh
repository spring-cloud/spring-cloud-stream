#!/bin/bash

if [ "$#" -eq 1 ]; then
  ARGVALUE=$1
  if [[ $ARGVALUE == *"skipTest"* ]]; then
    echo "Skippping Tests"
    ./mvnw clean install -f binders/kafka-binder/pom.xml -DskipTests
  elif [[ $ARGVALUE == *"disable.checks"* ]]; then
    echo "Skippping checkstyle checks"
    ./mvnw clean install -f binders/kafka-binder/pom.xml -Ddisable.checks=true
  fi
else
  ./mvnw clean install -f binders/kafka-binder/pom.xml
fi

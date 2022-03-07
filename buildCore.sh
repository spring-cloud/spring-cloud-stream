#!/bin/bash

if [ "$#" -eq 1 ]; then
  ARGVALUE=$1
  if [[ $ARGVALUE == *"skipTest"* ]]; then
    echo "Skippping Tests"
    ./mvnw clean install -f core/pom.xml -DskipTests
  fi 
else
  ./mvnw clean install -f core/pom.xml
fi

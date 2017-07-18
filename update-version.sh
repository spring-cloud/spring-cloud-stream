#!/bin/bash

#Execute this script from local checkout of spring cloud stream

./mvnw versions:update-parent -DparentVersion=[0.0.1,$2] -Pspring -DgenerateBackupPoms=false -DallowSnapshots=true
./mvnw versions:set -DnewVersion=$1 -DgenerateBackupPoms=false



lines=$(find . -name 'pom.xml' | xargs egrep "SNAPSHOT|M[0-9]|RC[0-9]" | grep -v regex | wc -l)
if [ $lines -eq 0 ]; then
	echo "No snapshots found"
else
	echo "Snapshots found."
fi

lines=$(find . -name 'pom.xml' | xargs egrep "M[0-9]" | grep -v regex | wc -l)
if [ $lines -eq 0 ]; then
	echo "No milestones found"
else
	echo "Milestones found."
fi

lines=$(find . -name 'pom.xml' | xargs egrep "RC[0-9]" | grep -v regex | wc -l)
if [ $lines -eq 0 ]; then
	echo "No release candidates found"
else
	echo "Release candidates found."
fi

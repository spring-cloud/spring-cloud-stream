Spring Cloud Stream Source Sample
=============================

In this *Spring Cloud Stream* sample, a timestamp is published on an interval determined by the fixedDelay property.

## Requirements

To run this sample, you will need to have installed:

* Java 8 or Above

This example requires Redis to be running on localhost.

## Code Tour

This sample is a Spring Boot application that uses Spring Cloud Stream to publish timestamp data. The source module has 3 primary components:

* SourceApplication - the Spring Boot Main Application
* TimeSource - the module that will generate the timestamp and post the message to the stream
* TimeSourceOptionsMetadata - defines the configurations that are available to setup the TimeSource
    	 * format - how to render the current time, using SimpleDateFormat
    	 * fixedDelay - time delay between messages
    	 * initialDelay - delay before the first message
    	 * timeUnit - the time unit for the fixed and initial delays
    	 * maxMessages - the maximum messages per poll; -1 for unlimited

## Building with Maven

Build the sample by executing:

	source>$ mvn clean package

## Running the Sample

To start the source module execute the following:

	source>$ java -jar target/spring-cloud-stream-sample-source-1.0.0.BUILD-SNAPSHOT-exec.jar


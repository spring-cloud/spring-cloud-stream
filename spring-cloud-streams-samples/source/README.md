Spring Cloud Streams Source Sample
=============================

In this sample for *Spring Cloud Streams* we will post a timestamp to the stream on an interval determined by the fixedDelay configuration.

## Requirements

In order for the sample to run you will need to have installed:

* Java 7 or Above

## Code Tour

This sample is a Spring Boot application that uses Spring Cloud Streams to send time stamp data to a stream. The source module has 3 primary components

* SourceApplication - in which is the Spring Boot Main Application
* TimeSource - the module that will generate the timestamp and post the message to the stream
* TimeSourceOptionsMetadata - defines the configurations that are available to setup the TimeSource
    	 * format - how to render the current time, using SimpleDateFormat
    	 * fixedDelay - time delay between messages, expressed in TimeUnits (seconds by default)
    	 * initialDelay - an initial delay when using a fixed delay trigger, expressed in TimeUnits (seconds by default)
    	 * timeUnit - the time unit for the fixed and initial delays
    	 * maxMessages - the maximum messages per poll; -1 for unlimited

## Building with Maven

Build the sample simply by executing:

	source>$ mvn clean package

## Running the Sample

This example requires a local instance of Redis to be up an running.
To start the source module execute the following:

	source>$ java -jar target/spring-cloud-streams-sample-source-1.0.0.BUILD-SNAPSHOT-exec.jar


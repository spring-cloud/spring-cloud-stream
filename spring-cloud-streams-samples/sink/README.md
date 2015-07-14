Spring Cloud Streams Sink Sample
=============================

In this sample for *Spring Cloud Streams* this sink module will receive messages from the stream and write the payload to console.

## Requirements

In order for the sample to run you will need to have installed:

* Java 7 or Above

## Code Tour

This sample is a Spring Boot application that uses Spring Cloud Streams to retrieve data from a stream and write result to the console. The sink module has 2 primary components

* SinkApplication - in which is the Spring Boot Main Application
* LogSink - the module that receives the data from the stream and writes it out to console.
## Building with Maven

Build the sample simply by executing:

	sink>$ mvn clean package

## Running the Sample

This example requires a local instance of Redis to be up an running.
To start the sink module execute the following:

	sink>$ java -jar target/spring-cloud-streams-sample-sink-1.0.0.BUILD-SNAPSHOT-exec.jar


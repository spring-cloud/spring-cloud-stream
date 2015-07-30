Spring Cloud Stream Sink Sample
=============================

In this *Spring Cloud Stream* sample, messages are received from a stream and the payload of each is logged to the console.

## Requirements

To run this sample, you will need to have installed:

* Java 8 or Above

This example requires Redis to be running on localhost.

## Code Tour

This sample is a Spring Boot application that uses Spring Cloud Stream to receive messages and write each payload to the console. The sink module has 2 primary components:

* SinkApplication - the Spring Boot Main Application
* LogSink - the module that receives the data from the stream and writes it out to the console

## Building with Maven

Build the sample by executing:

	sink>$ mvn clean package

## Running the Sample

To start the sink module execute the following:

	sink>$ java -jar target/spring-cloud-stream-sample-sink-1.0.0.BUILD-SNAPSHOT-exec.jar


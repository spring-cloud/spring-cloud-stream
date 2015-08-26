# Module Launcher

The Module Launcher provides a single entry point that bootstraps module JARs located in a Maven repository. A single Docker image can then be used to launch any of those JARs based on an environment variable. When running standalone, a system property may be used instead of an environment variable, so that multiple instances of the Module Launcher may run on a single machine. The following examples demonstrate running the modules for the *ticktock* stream (`time-source | log-sink`).

## Prerequisites

1: clone and build the spring-cloud-stream project:

````
git clone https://github.com/spring-cloud/spring-cloud-stream.git
cd spring-cloud-stream
mvn package
cd ..
````

2: start redis locally via `redis-server` or `docker-compose` (there's a `docker-compose.yml` in `spring-cloud-stream-samples`). Optionally start `redis-cli` and use the `MONITOR` command to watch activity.

*NOTE:* redis.conf (on OSX it is found here: /usr/local/etc/redis.conf) may need to be updated to set the binding to an address other than 127.0.0.1 else the docker instances will fail to connect. For example: bind 0.0.0.0

## Running Standalone

From the `spring-cloud-stream/spring-cloud-stream-module-launcher` directory:

````
java -Dmodules=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -Dspring.cloud.stream.bindings.output=ticktock -jar target/spring-cloud-stream-module-launcher-1.0.0.BUILD-SNAPSHOT.jar
java -Dmodules=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT -Dserver.port=8081 -Dspring.cloud.stream.bindings.input=ticktock -jar target/spring-cloud-stream-module-launcher-1.0.0.BUILD-SNAPSHOT.jar
````

Note that `server.port` needs to be specified explicitly for sink module as the time source module already uses the default port `8080`.
The binding property is set to use the same name `ticktock` for both the output/input bindings of source/sink modules so that log sink receives messages from time source.

The time messages will be emitted every 5 seconds. The console for the log module will display each:

````
2015-07-31 11:51:42.133  INFO 3388 --- [hannel-adapter1] sink.LogSink         : Received: 2015-07-31 11:51:36
2015-07-31 11:51:42.135  INFO 3388 --- [hannel-adapter1] sink.LogSink         : Received: 2015-07-31 11:51:41
2015-07-31 11:51:46.569  INFO 3388 --- [hannel-adapter1] sink.LogSink         : Received: 2015-07-31 11:51:46
````

*NOTE:* the two modules will be launched within a single process if both are provided (comma-delimited) via `-Dmodules`

## Running with Docker

The easiest way to get a demo working is to use `docker-compose` (From the `spring-cloud-stream/spring-cloud-stream-module-launcher` directory):

Make sure to set `DOCKER_HOST`. If you are running `boot2docker` VM, $(boot2docker shellinit) would set that up.

```
$ mvn package docker:build
$ docker-compose up
...
logsink_1    | 2015-08-11 08:25:49.909  INFO 1 --- [hannel-adapter1] o.s.cloud.stream.module.log.LogSink      : Received: 2015-08-11 08:25:49
logsink_1    | 2015-08-11 08:25:54.909  INFO 1 --- [hannel-adapter1] o.s.cloud.stream.module.log.Log
...
```

You can also run each module individually as a Docker process by passing environment variables for the module name as well as the host machine's IP address for the redis connection to be established within the container:
To find out redis host IP:
```
Get the container ID of redis: `docker ps`
Get the IP address by inspecting the container: `docker inspect <containerID>`

```
To run the modules individually on docker:
````
docker run -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT \
 -e spring.cloud.stream.bindings.output=ticktock -e SPRING_REDIS_HOST=<Redis-Host-IP> springcloud/stream-module-launcher

docker run -e MODULES=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT \
  -e spring.cloud.stream.bindings.input=ticktock -e SPRING_REDIS_HOST=<Redis-Host-IP> springcloud/stream-module-launcher
````
Note the binding name `ticktock` is specified for the source's output and sink's input.

To run pub/sub modules individually on docker, the binding name has to start with `topic:`.

````
docker run -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT \
 -e spring.cloud.stream.bindings.output=topic:foo -e SPRING_REDIS_HOST=<Redis-Host-IP> springcloud/stream-module-launcher

docker run -e MODULES=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT \
 -e spring.cloud.stream.bindings.input=topic:foo -e SPRING_REDIS_HOST=<Redis-Host-IP> springcloud/stream-module-launcher

docker run -e MODULES=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT \
 -e spring.cloud.stream.bindings.input=topic:foo -e SPRING_REDIS_HOST=<Redis-Host-IP> springcloud/stream-module-launcher
````
In the above scenario, both the sink modules receive the same messages from time source.

## Running on Lattice

1: Launch lattice with vagrant as described [here](http://lattice.cf/docs/getting-started/).

2: Create a Redis instance on Lattice (running as root):

````
$ ltc create redis redis -r
````

3: Run the modules as long-running processes (LRPs) on Lattice:

````
$ ltc create time springcloud/stream-module-launcher -m 512 \
 -e SPRING_CLOUD_STREAM_BINDINGS_OUTPUT=ticktock -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT
$ ltc create log springcloud/stream-module-launcher -m 512 \
 -e SPRING_CLOUD_STREAM_BINDINGS_INPUT=ticktock -e MODULES=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT
````

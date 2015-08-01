# Module Launcher

The Module Launcher provides a single entry point that bootstraps module JARs located in a Maven repository. A single Docker image can then be used to launch any of those JARs based on an environment variable. When running standalone, a system property may be used instead of an environment variable, so that multiple instances of the Module Launcher may run on a single machine. The following examples demonstrate running the modules for the *ticktock* stream (`time-source | log-sink`).

## Prerequisites

1: clone and build the spring-cloud-stream project:

````
git clone https://github.com/spring-cloud/spring-cloud-stream.git
cd spring-cloud-stream
mvn -s .settings.xml package
cd ..
````

2: start redis locally via `redis-server` (optionally start `redis-cli` and use the `MONITOR` command to watch activity)

*NOTE:* redis.conf (on OSX it is found here: /usr/local/etc/redis.conf) may need to be updated to set the binding to an address other than 127.0.0.1 else the docker instances will fail to connect. For example: bind 0.0.0.0

## Running Standalone

From the `spring-cloud-stream/spring-cloud-stream-module-launcher` directory:

````
java -Dmodules=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -jar target/spring-cloud-stream-module-launcher-1.0.0.BUILD-SNAPSHOT-exec.jar
java -Dmodules=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT -jar target/spring-cloud-stream-module-launcher-1.0.0.BUILD-SNAPSHOT-exec.jar
````

The time messages will be emitted every 5 seconds. The console for the log module will display each:

````
2015-07-31 11:51:42.133  INFO 3388 --- [hannel-adapter1] sink.LogSink         : Received: 2015-07-31 11:51:36
2015-07-31 11:51:42.135  INFO 3388 --- [hannel-adapter1] sink.LogSink         : Received: 2015-07-31 11:51:41
2015-07-31 11:51:46.569  INFO 3388 --- [hannel-adapter1] sink.LogSink         : Received: 2015-07-31 11:51:46
````

*NOTE:* the two modules will be launched within a single process if both are provided (comma-delimited) via `-Dmodules`

## Running with Docker

1: run each module as a docker process by passing environment variables for the module name as well as the host machine's IP address for the redis connection to be established within the container:

````
docker run -p 8080:8080 -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -e SPRING_REDIS_HOST=<host.ip> springcloud/stream-module-launcher
docker run -p 8081:8081 -e MODULES=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT -e SPRING_REDIS_HOST=<host.ip> springcloud/stream-module-launcher
````

## Running on Lattice

1: Launch lattice with vagrant as described [here](http://lattice.cf/docs/getting-started/).

2: Create a Redis instance on Lattice (running as root):

````
$ ltc create redis redis -r
````

3: Run the modules as long-running processes (LRPs) on Lattice:

````
$ ltc create time springcloud/stream-module-launcher -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -e SPRING_PROFILES_ACTIVE=cloud
$ ltc create log springcloud/stream-module-launcher -p 8081 -e MODULES=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT -e SPRING_PROFILES_ACTIVE=cloud
````

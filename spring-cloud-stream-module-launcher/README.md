# Module Launcher

The Module Launcher provides a single entry point that bootstraps module JARs located in a home directory. A single Docker image that contains such a directory of module JARs can then be used to launch any of those JARs based on an environment variable. When running standalone, a system property may be used instead of an environment variable, so that multiple instances of the Module Launcher may run on a single machine. The following examples demonstrate running the modules for the *ticktock* stream (`time | log`).

## Prerequisites

1: clone and build the spring-cloud-stream project:

````
git clone https://github.com/spring-cloud/spring-cloud-stream.git
cd spring-cloud-stream
mvn -s .settings.xml package
cd ..
````

2: copy the spring-cloud-stream source and sink sample JARs to `/opt/spring/modules`:

````
mkdir -p /opt/spring/modules/org/springframework/cloud/stream/module
cp spring-cloud-stream/spring-cloud-stream-samples/source/target/spring-cloud-stream-sample-source-1.0.0.BUILD-SNAPSHOT-exec.jar /opt/spring/modules/org/springframework/cloud/stream/module
cp spring-cloud-stream/spring-cloud-stream-samples/sink/target/spring-cloud-stream-sample-sink-1.0.0.BUILD-SNAPSHOT-exec.jar /opt/spring/modules/org/springframework/cloud/stream/module
````

3: start redis locally via `redis-server` (optionally start `redis-cli` and use the `MONITOR` command to watch activity)

*NOTE:* redis.conf (on OSX it is found here: /usr/local/etc/redis.conf) may need to be updated to set the binding to an address other than 127.0.0.1 else the docker instances will fail to connect. For example: bind 0.0.0.0

## Running Standalone

From the `spring-cloud-stream/spring-cloud-stream-module-launcher` directory:

````
java -Dmodules=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -jar target/spring-cloud-stream-module-launcher-1.0.0.BUILD-SNAPSHOT-exec.jar
java -Dmodules=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT -jar target/spring-cloud-stream-module-launcher-1.0.0.BUILD-SNAPSHOT-exec.jar
````

The time messages will be emitted every 5 seconds. The console for the log module will display each:

````
2015-06-05 12:39:58.896  INFO 51078 --- [hannel-adapter1] config.ModuleDefinition                  : Received: 2015-06-05 12:39:58
2015-06-05 12:40:02.699  INFO 51078 --- [hannel-adapter1] config.ModuleDefinition                  : Received: 2015-06-05 16:39:52
2015-06-05 12:40:03.897  INFO 51078 --- [hannel-adapter1] config.ModuleDefinition                  : Received: 2015-06-05 12:40:03
````

*NOTE:* the two modules will be launched within a single process if both are specified: `-Dmodules=time,log`

## Running with Docker

1: build the module-launcher Docker image, including a copy of the module directory:

From the `spring-cloud-stream/spring-cloud-stream-module-launcher` directory:

````
mkdir artifacts
cp -r /opt/spring/modules artifacts/
./dockerize.sh
````

2: run each module as a docker process by passing environment variables for the module name as well as the host machine's IP address for the redis connection to be established within the container:

````
docker run -p 8080:8080 -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -e SPRING_REDIS_HOST=<host.ip> 192.168.59.103:5000/module-launcher
docker run -p 8081:8081 -e MODULES=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT -e SPRING_REDIS_HOST=<host.ip> 192.168.59.103:5000/module-launcher
````

## Running on Lattice

### Initial Setup (if necessary)

1: Launch lattice with vagrant as described [here](http://lattice.cf/docs/getting-started/).

2: Run a private Docker registry, and configure Lattice to use that as described [here](http://lattice.cf/docs/private-docker-registry/).

### Deploying Modules

1: Push the Docker image to the private registry (if necessary, run `$(boot2docker shellinit)` first):

````
$ docker push 192.168.59.103:5000/module-launcher
````

2: Create a Redis instance on Lattice (running as root):

````
$ ltc create redis redis -r
````

3: Run the modules as long-running processes (LRPs) on Lattice:

````
$ ltc create time 192.168.59.103:5000/module-launcher -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -e SPRING_PROFILES_ACTIVE=cloud
$ ltc create log 192.168.59.103:5000/module-launcher -e MODULES=org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT -e SPRING_PROFILES_ACTIVE=cloud
````

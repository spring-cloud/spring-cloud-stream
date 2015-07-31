FROM java:8
VOLUME /tmp
ADD target/spring-cloud-stream-module-launcher-1.0.0.BUILD-SNAPSHOT-exec.jar module-launcher.jar
RUN bash -c 'touch /module-launcher.jar'
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/module-launcher.jar"]

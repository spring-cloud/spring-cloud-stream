FROM java:8
VOLUME /tmp
ADD *.jar module-launcher.jar
RUN bash -c 'touch /module-launcher.jar'
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/module-launcher.jar"]

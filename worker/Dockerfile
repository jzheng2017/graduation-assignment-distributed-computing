FROM openjdk:17
EXPOSE 8081
ADD target/worker-1.0-SNAPSHOT.jar worker.jar
ENTRYPOINT ["java", "-jar", "worker.jar"]

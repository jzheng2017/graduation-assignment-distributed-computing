FROM openjdk:17
EXPOSE 8082
ADD target/consumer-coordinator-1.0-SNAPSHOT.jar consumer-coordinator.jar
ENTRYPOINT ["java", "-jar", "consumer-coordinator.jar"]

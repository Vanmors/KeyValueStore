FROM openjdk:21-jdk-slim
COPY target/KeyValueStore-1.0-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app.jar"]
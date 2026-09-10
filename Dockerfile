FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

FROM openjdk:21-jdk-slim
WORKDIR /app
COPY --from=build /src/target/*.jar /app/app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]
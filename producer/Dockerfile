FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/target/commit-log-producer-1.0.0.jar /app/commit-log-producer.jar
ENTRYPOINT ["java", "-jar", "/app/commit-log-producer.jar"]

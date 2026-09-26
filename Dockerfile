# Multi-stage: build the jar, then run it on a JRE. -Xmx256m is set at run time in
# docker-compose to demonstrate the flat-heap claim — the app ingests 5M rows without
# raising the heap because the reader streams and the writer batches.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/target/batch-integration-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]

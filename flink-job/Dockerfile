FROM maven:3.9.9-eclipse-temurin-17 AS deps
WORKDIR /workspace
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline

FROM deps AS test
COPY src ./src
RUN mvn test

FROM deps AS build
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:2.1.2-scala_2.12-java17 AS runtime
COPY --from=build /workspace/target/realtime-lab-flink-job-1.0.0.jar /opt/flink/usrlib/realtime-lab-flink-job.jar

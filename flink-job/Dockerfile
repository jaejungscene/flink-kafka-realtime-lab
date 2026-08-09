FROM maven:3.9.9-eclipse-temurin-17 AS deps
WORKDIR /workspace
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline

FROM deps AS test
COPY src ./src
RUN mvn -B -ntp verify

FROM deps AS build
COPY src ./src
RUN mvn -q -DskipTests package

FROM deps AS plugins
RUN mvn -q dependency:copy \
    -Dartifact=org.apache.flink:flink-metrics-prometheus:2.1.2 \
    -DoutputDirectory=/plugins

FROM flink:2.1.2-scala_2.12-java17 AS runtime
COPY --chown=flink:flink --from=build /workspace/target/realtime-lab-flink-job-1.0.0.jar /opt/flink/usrlib/realtime-lab-flink-job.jar
COPY --chown=flink:flink --from=plugins /plugins/flink-metrics-prometheus-2.1.2.jar /opt/flink/plugins/metrics-prometheus/
COPY --chown=flink:flink docker/submit-job.sh /opt/flink/bin/submit-job.sh
RUN chmod 0755 /opt/flink/bin/submit-job.sh \
    && mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/ \
    && mkdir -p /opt/flink/state/checkpoints /opt/flink/state/savepoints \
    && chown -R flink:flink /opt/flink/state /opt/flink/plugins/s3-fs-hadoop

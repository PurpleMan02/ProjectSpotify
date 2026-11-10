FROM apache/airflow:2.9.3

COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
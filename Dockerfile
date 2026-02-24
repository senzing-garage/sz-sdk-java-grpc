# -----------------------------------------------------------------------------
# Stages
# -----------------------------------------------------------------------------

ARG IMAGE_BUILDER=debian:13.2-slim@sha256:18764e98673c3baf1a6f8d960b5b5a1ec69092049522abac4e24a7726425b016
#ARG IMAGE_FINAL=senzing/senzingsdk-tools:4.1.0@sha256:89a7285056f820ca56048527c4df022e0e4db407e13dcc0d2f321e69f76d5b9c
ARG IMAGE_FINAL=senzing/senzingsdk-runtime:4.2.0@sha256:072ff062d9d3ee224e68848e8a37b9f4d6db9ada016fdd0313c3a5bd946df8b9

# -----------------------------------------------------------------------------
# Stage: senzingsdk_runtime
# -----------------------------------------------------------------------------

FROM ${IMAGE_FINAL} AS senzingsdk_runtime

RUN apt-get update \
       && apt-get -y --no-install-recommends install \
       senzingsdk-setup

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------
FROM ${IMAGE_BUILDER} AS builder
ENV REFRESHED_AT=2026-02-24
LABEL Name="senzing/java-builder" \
       Maintainer="support@senzing.com" \
       Version="0.5.1"

# Run as "root" for system installation.

USER root

# Copy files from prior stage.

COPY --from=senzingsdk_runtime  "/opt/senzing/"   "/opt/senzing/"
COPY --from=senzingsdk_runtime  "/etc/opt/senzing/"   "/etc/opt/senzing/"

# Install packages via apt-get.

RUN apt-get update \
       && apt-get -y --no-install-recommends install \
       libsqlite3-dev \
       apt-transport-https \
       gnupg2 \
       gpg \
       wget \
       && apt-get install -y --reinstall ca-certificates \
       && update-ca-certificates \
       && apt-get clean \
       && rm -rf /var/lib/apt/lists/*

# Install Java-17.

RUN wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null \
       && echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list

RUN apt-get update \
       && apt-get install -y --no-install-recommends temurin-17-jdk \
       && rm -rf /var/lib/apt/lists/*

RUN wget https://archive.apache.org/dist/maven/maven-3/3.9.12/binaries/apache-maven-3.9.12-bin.tar.gz -P /opt \
       && tar xf /opt/apache-maven-*.tar.gz -C /opt \
       && ln -s /opt/apache-maven-3.9.12 /opt/maven

ENV M2_HOME=/opt/maven
ENV MAVEN_HOME=/opt/maven
ENV PATH=${PATH}:${M2_HOME}/bin

# Copy local files from the Git repository.

# COPY ./rootfs /
COPY . /sz-sdk-java-grpc
#COPY --from=m2repo . /root/.m2/repository

# Set path to Senzing libs.

ENV SENZING_PATH=/opt/senzing
ENV LD_LIBRARY_PATH=/opt/senzing/er/lib/

# Build the jar file

WORKDIR /opt/senzing/er/sdk/java
RUN java -jar sz-sdk.jar -x
WORKDIR /sz-sdk-java-grpc
#RUN find /opt/senzing -type d
#RUN find /etc/opt/senzing -type d
RUN mvn -ntp -DskipTests=true package

# -----------------------------------------------------------------------------
# Stage: final
# -----------------------------------------------------------------------------

FROM ${IMAGE_FINAL} AS final
ENV REFRESHED_AT=2026-02-24
LABEL Name="senzing/sz-sdk-grpc-java" \
       Maintainer="support@senzing.com" \
       Version="0.5.1"
#HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD ["/app/healthcheck.sh"]
HEALTHCHECK CMD ["echo hello"]
USER root

# Install packages via apt-get.

RUN apt-get update \
       && apt-get -y --no-install-recommends install \
       senzingsdk-setup \
       libsqlite3-dev \
       apt-transport-https \
       gnupg2 \
       gpg \
       wget \
       && apt-get install -y --reinstall ca-certificates \
       && update-ca-certificates \
       && apt-get clean \
       && rm -rf /var/lib/apt/lists/*

# Install Java-17.

RUN wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null \
       && echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list

RUN apt-get update \
       && apt-get install -y --no-install-recommends temurin-17-jdk \
       && rm -rf /var/lib/apt/lists/*

# Copy files from repository.

#COPY ./rootfs /

# Copy files from prior stage.

COPY --from=builder /sz-sdk-java-grpc/target/sz-sdk-grpc-server.jar /app/sz-sdk-grpc-server.jar

# Run as non-root container

USER 1001

# Runtime environment variables.
ENV SENZING_PATH=/opt/senzing
ENV LD_LIBRARY_PATH=/opt/senzing/er/lib/
ENV SENZING_TOOLS_CORE_DATABASE_URI=sqlite3:///tmp/senzing-repo.db

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["java","-jar","/app/sz-sdk-grpc-server.jar"]
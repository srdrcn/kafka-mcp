# Multi-stage: build with Maven, run as non-root on the UBI9 OpenJDK 21 runtime.
# OpenShift restricted-v2 SCC: random UID + GID 0 (chgrp 0 / g=u), read-only root fs, /tmp emptyDir.
# For reproducible builds pin both base images to a version tag or digest, e.g.
#   make image BUILD_ARGS="--build-arg RUNTIME_IMAGE=registry.access.redhat.com/ubi9/openjdk-21-runtime@sha256:<digest>"
ARG BUILDER_IMAGE=registry.access.redhat.com/ubi9/openjdk-21:latest
ARG RUNTIME_IMAGE=registry.access.redhat.com/ubi9/openjdk-21-runtime:latest

FROM ${BUILDER_IMAGE} AS build
USER 0
# mvnw unpacks the Maven distribution with tar xz (no unzip in the image), which needs gzip
RUN microdnf install -y gzip && microdnf clean all
WORKDIR /build
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw -B -q dependency:go-offline -DexcludeGroupIds=io.github.srdrcn || true
COPY src/ src/
RUN ./mvnw -B -q package -DskipTests && cp target/kafka-mcp.jar /build/kafka-mcp.jar

FROM ${RUNTIME_IMAGE}
USER 0
RUN mkdir -p /deployments /etc/kafka-mcp/config /etc/kafka-mcp/krb5 /etc/kafka-mcp/keytabs \
 && chgrp -R 0 /deployments /etc/kafka-mcp \
 && chmod -R g=u /deployments /etc/kafka-mcp
COPY --from=build --chown=185:0 /build/kafka-mcp.jar /deployments/kafka-mcp.jar
RUN chmod g=u /deployments/kafka-mcp.jar

ENV JAVA_TOOL_OPTIONS="-Djava.security.krb5.conf=/etc/kafka-mcp/krb5/krb5.conf -Djava.io.tmpdir=/tmp -XX:MaxRAMPercentage=75"
WORKDIR /deployments
USER 185
EXPOSE 8080
# Exit on OutOfMemoryError so that OpenShift restarts the pod instead of keeping a half-broken JVM
ENTRYPOINT ["java", "-XX:+ExitOnOutOfMemoryError", "-jar", "/deployments/kafka-mcp.jar"]
CMD ["--config", "/etc/kafka-mcp/config/kafka-mcp.yaml"]

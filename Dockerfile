# Multi-stage: Maven ile derle, UBI9 OpenJDK 21 runtime üzerinde non-root çalıştır.
# OpenShift restricted-v2 SCC: rastgele UID + GID 0 (chgrp 0 / g=u), salt-okunur root fs, /tmp emptyDir.
ARG BUILDER_IMAGE=registry.access.redhat.com/ubi9/openjdk-21:latest
ARG RUNTIME_IMAGE=registry.access.redhat.com/ubi9/openjdk-21-runtime:latest

FROM ${BUILDER_IMAGE} AS build
USER 0
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
ENTRYPOINT ["java", "-jar", "/deployments/kafka-mcp.jar"]
CMD ["--config", "/etc/kafka-mcp/config/kafka-mcp.yaml"]

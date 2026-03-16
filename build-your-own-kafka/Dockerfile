FROM maven:3.9.9-eclipse-temurin-25-alpine AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests


FROM eclipse-temurin:25-jdk-alpine AS layers

WORKDIR /layer
COPY --from=builder /build/target/*.jar app.jar
RUN java -Djarmode=layertools -jar app.jar extract


FROM eclipse-temurin:25-jdk-alpine AS jre-builder


RUN jlink \
    --add-modules \
        java.base,\
        java.logging,\
        java.naming,\
        java.management,\
        java.security.jgss,\
        java.instrument,\
        java.net.http,\
        java.sql,\
        jdk.unsupported,\
        jdk.crypto.ec \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --compress=2 \
    --output /custom-jre


FROM alpine:3.23

# Security: non-root user
RUN addgroup -S kafka && adduser -S kafka -G kafka

# Copy our custom minimal JRE from stage 3
COPY --from=jre-builder /custom-jre /opt/jre

# Add JRE to PATH
ENV PATH="/opt/jre/bin:${PATH}" \
    JAVA_HOME="/opt/jre"

WORKDIR /app
RUN chown kafka:kafka /app

# Copy layered JAR contents from stage 2
COPY --from=layers --chown=kafka:kafka /layer/dependencies/ ./
COPY --from=layers --chown=kafka:kafka /layer/spring-boot-loader/ ./
COPY --from=layers --chown=kafka:kafka /layer/snapshot-dependencies/ ./
COPY --from=layers --chown=kafka:kafka /layer/application/ ./

USER kafka

EXPOSE 9092

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:9092 || exit 1

ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
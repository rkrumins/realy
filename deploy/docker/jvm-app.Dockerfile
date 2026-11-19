# Runtime image for the JVM services (gateway, mock-model-server, load-emitter).
# Build the distribution first: (cd java && ./gradlew installDist)
FROM eclipse-temurin:21-jre
ARG APP
COPY ${APP}/build/install/${APP} /app
RUN ln -s /app/bin/${APP} /app/run
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -XX:MaxRAMPercentage=70"
WORKDIR /app
ENTRYPOINT ["/app/run"]

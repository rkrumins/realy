# syntax=docker/dockerfile:1.7
# realy — every image built from source with Docker alone (context = repository root):
#   docker compose -f deploy/compose/docker-compose.yml up -d --build        (or: make up)
# Targets: gateway | mock-model | flink | console (also: java-build, ui-build intermediate stages).
# Behind a TLS-intercepting proxy: make up-proxy (deploy/compose/docker-compose.proxy.yml).

ARG GRADLE_IMAGE=gradle:8.14-jdk21
ARG NODE_IMAGE=node:22-alpine
ARG PYTHON_IMAGE=python:3.13-slim
ARG JRE_IMAGE=eclipse-temurin:21-jre
ARG FLINK_IMAGE=flink:2.2.1-java21

# ------------------------------------------------------------------------------- Java build
FROM ${GRADLE_IMAGE} AS java-build
ARG HTTPS_PROXY
# Optional Maven Central mirror (corporate Nexus/Artifactory, or a public mirror to dodge rate limits).
ARG MAVEN_MIRROR=
COPY deploy/docker/build-trust.sh /usr/local/bin/realy-build-trust
WORKDIR /src
COPY java/ java/
COPY contracts/ contracts/
COPY rules/ rules/
COPY models/proxy/default.weights.json models/proxy/default.weights.json
RUN --mount=type=secret,id=ca,required=false --mount=type=cache,target=/root/.gradle/caches \
    sh /usr/local/bin/realy-build-trust \
 && MAVEN_MIRROR="$MAVEN_MIRROR" gradle -p java --no-daemon --console=plain -q --max-workers=2 \
      -Dorg.gradle.internal.repository.max.retries=10 -Dorg.gradle.internal.repository.initial.backoff=2000 \
      installDist :flink-job:shadowJar -x test

# ------------------------------------------------------------------------------- UI build
FROM ${NODE_IMAGE} AS ui-build
ARG HTTPS_PROXY
COPY deploy/docker/build-trust.sh /usr/local/bin/realy-build-trust
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ui/.npmrc ./
RUN --mount=type=secret,id=ca,required=false --mount=type=cache,target=/root/.npm \
    sh /usr/local/bin/realy-build-trust \
 && if [ -s /run/secrets/ca ]; then export NODE_EXTRA_CA_CERTS=/run/secrets/ca; fi \
 && npm ci --no-audit --no-fund
COPY ui/ ./
RUN VITE_OUT_DIR=/ui/dist npm run build

# ------------------------------------------------------------------------------- services
FROM ${JRE_IMAGE} AS jvm-base
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -XX:MaxRAMPercentage=70"
WORKDIR /app

FROM jvm-base AS gateway
COPY --from=java-build /src/java/scoring-gateway/build/install/scoring-gateway /app
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=30 CMD curl -fsS http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["/app/bin/scoring-gateway"]

FROM jvm-base AS mock-model
COPY --from=java-build /src/java/mock-model-server/build/install/mock-model-server /app
COPY models/proxy /models/proxy
EXPOSE 8001 8002
HEALTHCHECK --interval=5s --timeout=3s --retries=30 CMD curl -fsS http://127.0.0.1:8002/health || exit 1
ENTRYPOINT ["/app/bin/mock-model-server"]

# Flink: the job jar on the system classpath (parent-first) — one classloader for the TaskManager's
# lifetime, so job restarts never leak a user-code classloader. Also used by the Flink operator
# (jarURI local:///opt/flink/lib/realy/realy-flink-job.jar).
FROM ${FLINK_IMAGE} AS flink
COPY --from=java-build /src/java/flink-job/build/libs/realy-flink-job.jar /opt/flink/lib/realy/realy-flink-job.jar

# ------------------------------------------------------------------------------- console + driver
FROM ${JRE_IMAGE} AS jre
FROM ${PYTHON_IMAGE} AS console
ARG HTTPS_PROXY
# train: `realy train` runs in the console too (retrain -> redeploy -> measure without leaving Docker)
ARG EXTRAS=control,train
COPY deploy/docker/build-trust.sh /usr/local/bin/realy-build-trust
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:$PATH \
    REALY_HOME=/opt/realy REALY_RUNS=/runs REALY_DATA=/data REALY_SCENARIOS=/data/scenarios \
    PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /opt/realy
COPY python/pyproject.toml python/pyproject.toml
COPY python/realy python/realy
COPY python/kserve python/kserve
RUN --mount=type=secret,id=ca,required=false --mount=type=cache,target=/root/.cache/pip \
    sh /usr/local/bin/realy-build-trust \
 && if [ -s /run/secrets/ca ]; then export PIP_CERT=/run/secrets/ca; fi \
 && pip install -e "python[${EXTRAS}]"
COPY contracts contracts
COPY rules rules
COPY scenarios scenarios
COPY models models
COPY docs docs
COPY README.md README.md
COPY --from=java-build /src/java/load-emitter/build/install/load-emitter java/load-emitter/build/install/load-emitter
COPY --from=java-build /src/java/flink-job/build/libs/realy-flink-job.jar java/flink-job/build/libs/realy-flink-job.jar
COPY --from=ui-build /ui/dist python/realy/control/static
COPY deploy/docker/console-entrypoint.sh /usr/local/bin/console-entrypoint
EXPOSE 8090
VOLUME ["/runs", "/data"]
HEALTHCHECK --interval=5s --timeout=3s --retries=60 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8090/api/health', timeout=2)" || exit 1
ENTRYPOINT ["sh", "/usr/local/bin/console-entrypoint"]
CMD ["console", "--host", "0.0.0.0", "--port", "8090"]

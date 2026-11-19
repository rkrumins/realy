# Bench driver + operator console image: Python tools, the Java gRPC load emitter (sync rail) and the
# contracts/scenarios/rules/models they read. Context = repository root.
#   docker build -f deploy/docker/bench.Dockerfile -t realy/bench:dev .
FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/realy
COPY contracts contracts
COPY rules rules
COPY scenarios scenarios
COPY models models
COPY python python
COPY java/load-emitter/build/install/load-emitter java/load-emitter/build/install/load-emitter
COPY java/flink-job/build/libs java/flink-job/build/libs
RUN pip install --no-cache-dir -e "python[control,train]"
ENV REALY_RUNS=/runs
ENTRYPOINT ["realy"]
CMD ["--help"]

#!/usr/bin/env bash
# TaskManager liveness: the JobManager lists this TaskManager as registered. A JVM that hung (a fatal
# error whose shutdown never finished) keeps the container "Up" while serving nothing; this check makes
# Docker see it as unhealthy. Prefer taskmanager.jvm-exit-on-oom so it exits and restarts instead.
set -euo pipefail
ip=$(hostname -i | awk '{print $1}')
curl -fsS --max-time 4 "http://${JOBMANAGER_HOST:-flink-jobmanager}:8081/taskmanagers" | grep -q "\"${ip}:"

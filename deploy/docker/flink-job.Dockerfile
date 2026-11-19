# Flink application image for the Flink Kubernetes Operator (jarURI local:///opt/flink/usrlib/...).
# Build the jar first: (cd java && ./gradlew :flink-job:shadowJar); context = java/
FROM flink:2.2.1-java21
COPY flink-job/build/libs/realy-flink-job.jar /opt/flink/usrlib/realy-flink-job.jar

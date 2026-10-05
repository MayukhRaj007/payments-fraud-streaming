# PyFlink runtime: the official Flink image plus Python, PyFlink, and the
# connector JARs the job needs.
#
# Note on Python: the Flink 1.20 image is Ubuntu 22.04, whose system Python is
# 3.10. PyFlink 1.20.5 supports 3.8-3.11, so 3.10 is fine, and the rest of the
# project runs 3.11. flink_jobs/rules.py is written to work on both -- notably
# it normalises the trailing 'Z' in timestamps itself, since fromisoformat only
# accepts it on 3.11+.
FROM apache/flink:1.20.5-scala_2.12-java11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    # PyFlink shells out to `python`, which Ubuntu does not provide by default.
    ln -sf /usr/bin/python3 /usr/bin/python

# Pinned to the exact Flink version in the base image. A mismatch here produces
# confusing Java/Python gateway errors rather than a clear failure.
RUN pip3 install --no-cache-dir apache-flink==1.20.5

# Connector JARs must sit in /opt/flink/lib so both JobManager and TaskManager
# load them. All three URLs were verified against Maven Central.
ARG KAFKA_CONNECTOR=3.4.0-1.20
ARG JDBC_CONNECTOR=3.3.0-1.20
ARG PG_DRIVER=42.7.4
RUN cd /opt/flink/lib && \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR}/flink-connector-jdbc-${JDBC_CONNECTOR}.jar" && \
    curl -fsSLO "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_DRIVER}/postgresql-${PG_DRIVER}.jar"

COPY rules.py fraud_detector.py /opt/pfs/
COPY submit.sh /opt/pfs/submit.sh
RUN chmod +x /opt/pfs/submit.sh

# Belt and braces: the job also appends its own directory to sys.path.
ENV PYTHONPATH=/opt/pfs

ENV PYTHONUNBUFFERED=1
USER flink

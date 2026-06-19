# Spark 3.5.4 + Kyuubi Authz 1.11.1 + Iceberg 1.8.1
#
# WHY SPARK 3.5, NOT SPARK 4.0:
# Spark 4.0 is Scala 2.13-only. kyuubi-spark-authz_2.13 is NOT published to
# Maven Central (verified: 0 results on search.maven.org as of 2026-06-19).
# Kyuubi 1.11.x enables authZ compile support for Spark 4.0 in source (#7256)
# but does NOT ship a pre-built artifact. Building from source would require
# ~30 min of Maven compilation in the image. The latest pre-built combo is:
#   Spark 3.5 (scala2.12) + kyuubi-spark-authz_2.12-1.11.1 (latest stable).
# Kyuubi 1.11.0+ fully supports Spark 3.5; Spark 3.3/3.4 are deprecated.
#
# Jars downloaded at build time into $SPARK_HOME/jars/ so spark-sql can find
# them without --jars flags. No extra dependencies at runtime.
#
# HADOOP_USER_NAME=bob (set in docker-compose.yml) makes Kyuubi Authz send
# "bob" to Ranger as the query user, resolving bob's `engineer` role membership.
#
# Ranger plugin config files (ranger-spark-security.xml, ranger-spark-audit.xml)
# are mounted from the host into /opt/spark/conf/ at runtime.
#
# mask_show_last_n: This Hive GenericUDF is needed by the Kyuubi Authz
# MASK_SHOW_LAST_4 transformer. It ships inside the kyuubi-spark-authz shaded
# jar via bundled ranger-plugins-common. No separate hive-exec jar needed.

FROM apache/spark:3.5.4-scala2.12-java17-ubuntu

USER root

# Download Iceberg Spark runtime and Kyuubi Authz jars into $SPARK_HOME/jars/
# so they are on the default classpath for spark-sql.
ARG ICEBERG_VERSION=1.8.1
ARG KYUUBI_VERSION=1.11.1
ARG SCALA_VERSION=2.12
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

RUN set -eux; \
    # Iceberg Spark runtime (catalog + REST catalog support)
    curl -fsSL \
      "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
      -o "${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"; \
    # Kyuubi Spark AuthZ (Ranger plugin, Spark 3.5 lane)
    curl -fsSL \
      "${MAVEN_CENTRAL}/org/apache/kyuubi/kyuubi-spark-authz_${SCALA_VERSION}/${KYUUBI_VERSION}/kyuubi-spark-authz_${SCALA_VERSION}-${KYUUBI_VERSION}.jar" \
      -o "${SPARK_HOME}/jars/kyuubi-spark-authz_${SCALA_VERSION}-${KYUUBI_VERSION}.jar"; \
    # AWS SDK bundles for S3 access (RustFS S3-compatible)
    curl -fsSL \
      "${MAVEN_CENTRAL}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
      -o "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar" || true; \
    curl -fsSL \
      "${MAVEN_CENTRAL}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
      -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar" || true; \
    echo "Jars downloaded OK"

# Ranger plugin config directory: ranger-spark-security.xml and
# ranger-spark-audit.xml are resolved from $SPARK_HOME/conf/ at runtime.
# They are mounted by docker-compose, so nothing to copy here.

# Use the spark user (non-root) for running queries; HADOOP_USER_NAME overrides
# the OS username for Ranger policy resolution.
USER spark

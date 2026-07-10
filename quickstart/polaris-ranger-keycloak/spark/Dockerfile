# Spark 3.5.4 + Kyuubi Authz 1.11.1 (shaded) + Iceberg 1.8.1
#
# WHY SPARK 3.5, NOT SPARK 4.0:
# Spark 4.0 is Scala 2.13-only. kyuubi-spark-authz_2.13 is NOT published to
# Maven Central (verified: 0 results on search.maven.org as of 2026-06-19).
# Kyuubi 1.11.x enables authZ compile support for Spark 4.0 in source (#7256)
# but does NOT ship a pre-built artifact. Building from source would require
# ~30 min of Maven compilation in the image. The latest pre-built combo is:
#   Spark 3.5 (scala2.12) + kyuubi-spark-authz-shaded_2.12-1.11.1 (latest stable).
# Kyuubi 1.11.0+ fully supports Spark 3.5; Spark 3.3/3.4 are deprecated.
#
# WHY THE SHADED JAR (kyuubi-spark-authz-shaded), NOT the plain authz jar:
# The plain kyuubi-spark-authz_2.12 jar contains only Kyuubi's own wrappers and
# delegates to the real Ranger plugin at runtime. With just that jar (plus
# ranger-plugins-common / ranger-plugin-classloader) the RangerSparkExtension
# loads RangerBasePlugin but then dies on a missing AuditProviderFactory, which
# lives in ranger-plugins-audit (yet another artifact). Chasing the audit jar
# and its transitive deps by hand is a rabbit hole. The shaded jar is Kyuubi's
# DOCUMENTED install artifact: it bundles ranger-plugins-common AND
# ranger-plugins-audit (so AuditProviderFactory resolves), plus the
# mask_show_last_n Hive UDF the MASK_SHOW_LAST_4 transformer needs. One jar,
# no manual ranger-jar hunt.
#   Docs: https://kyuubi.readthedocs.io/en/master/security/authorization/spark/install.html
#
# Jars downloaded at build time into $SPARK_HOME/jars/ so spark-sql can find
# them without --jars flags. No extra dependencies at runtime.
#
# HADOOP_USER_NAME=bob (set in docker-compose.yml) makes Kyuubi Authz send
# "bob" to Ranger as the query user, resolving bob's `engineer` role membership.
#
# Ranger plugin config files (ranger-spark-security.xml, ranger-spark-audit.xml)
# are mounted from the host into /opt/spark/conf/ at runtime.

FROM apache/spark:3.5.4-scala2.12-java17-ubuntu

USER root

# Download Iceberg Spark runtime + the shaded Kyuubi Authz jar (ranger deps
# bundled) into $SPARK_HOME/jars/ so they are on the default classpath for
# spark-sql.
ARG ICEBERG_VERSION=1.8.1
ARG KYUUBI_VERSION=1.11.1
ARG SCALA_VERSION=2.12
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

RUN set -eux; \
    # Iceberg Spark runtime (catalog + REST catalog support)
    curl -fsSL \
      "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
      -o "${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"; \
    # Kyuubi Spark AuthZ SHADED jar (Ranger plugin + ranger-plugins-common +
    # ranger-plugins-audit + mask UDF all bundled). This is the artifact that
    # resolves the AuditProviderFactory ClassNotFound the plain authz jar hits.
    curl -fsSL \
      "${MAVEN_CENTRAL}/org/apache/kyuubi/kyuubi-spark-authz-shaded_${SCALA_VERSION}/${KYUUBI_VERSION}/kyuubi-spark-authz-shaded_${SCALA_VERSION}-${KYUUBI_VERSION}.jar" \
      -o "${SPARK_HOME}/jars/kyuubi-spark-authz-shaded_${SCALA_VERSION}-${KYUUBI_VERSION}.jar"; \
    # AWS SDK v2 bundle for Iceberg S3FileIO (RustFS S3-compatible).
    # Iceberg's org.apache.iceberg.aws.s3.S3FileIO uses AWS SDK *v2*
    # (software.amazon.awssdk.*), NOT the v1 aws-java-sdk-bundle. Without this
    # the REST catalog loads but the first table read dies with
    # ClassNotFoundException: software.amazon.awssdk.services.s3.model.S3Exception.
    curl -fsSL \
      "${MAVEN_CENTRAL}/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar" \
      -o "${SPARK_HOME}/jars/aws-sdk-bundle-2.24.6.jar"; \
    # url-connection-client: lightweight HTTP client S3FileIO defaults to.
    curl -fsSL \
      "${MAVEN_CENTRAL}/software/amazon/awssdk/url-connection-client/2.24.6/url-connection-client-2.24.6.jar" \
      -o "${SPARK_HOME}/jars/aws-url-connection-client-2.24.6.jar"; \
    echo "Jars downloaded OK"

# Ranger plugin config directory: ranger-spark-security.xml and
# ranger-spark-audit.xml are resolved from $SPARK_HOME/conf/ at runtime.
# They are mounted by docker-compose, so nothing to copy here.

# Ranger plugin policy-cache dir (ranger-spark-security.xml points here). Create
# it writable by the spark user so the PolicyRefresher can persist its cache
# without "Permission denied" noise. Policies still load into memory either way.
RUN mkdir -p /tmp/ranger-policy-cache && chown -R spark:spark /tmp/ranger-policy-cache

# Use the spark user (non-root) for running queries; HADOOP_USER_NAME overrides
# the OS username for Ranger policy resolution.
USER spark

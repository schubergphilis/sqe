# syntax=docker/dockerfile:1
# Apache Arrow Flight SQL ODBC driver (apache-arrow-26.0.1) + pyodbc + probe script.
# Mirrors the project's ubuntu-cpp-odbc compose service (compose.yaml): BUNDLED deps.
FROM ubuntu:26.04@sha256:d66b366dac61b575bee2aef07e9c26e8070096fbe84a3d4d3eb0da069b5d76db AS build
ARG DEBIAN_FRONTEND=noninteractive
ARG ARROW_TAG=apache-arrow-26.0.1
RUN apt-get update -y -q && apt-get install -y -q --no-install-recommends \
      autoconf build-essential ca-certificates cmake curl git libssl-dev make \
      ninja-build patch pkg-config python3 unixodbc-dev zlib1g-dev xz-utils \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch ${ARROW_TAG} https://github.com/apache/arrow.git /arrow
RUN cmake -S /arrow/cpp -B /build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/opt/arrow \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DARROW_DEPENDENCY_SOURCE=BUNDLED \
      -DARROW_DEPENDENCY_USE_SHARED=OFF \
      -DARROW_FLIGHT=ON -DARROW_FLIGHT_SQL=ON -DARROW_COMPUTE=ON \
      -DARROW_FLIGHT_SQL_ODBC=ON \
      -DARROW_BUILD_TESTS=OFF -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=ON \
      -DARROW_ACERO=OFF -DARROW_AZURE=OFF -DARROW_CSV=OFF -DARROW_DATASET=OFF \
      -DARROW_GANDIVA=OFF -DARROW_GCS=OFF -DARROW_HDFS=OFF -DARROW_ORC=OFF \
      -DARROW_PARQUET=OFF -DARROW_S3=OFF -DARROW_SUBSTRAIT=OFF
# Everything except the ODBC objects builds cleanly; commit that progress first.
RUN cmake --build /build -j"$(nproc)" || true
# flight_sql_driver.cc includes <absl/synchronization/mutex.h> but the bundled Abseil
# headers are not on its include path; point the compiler at the bundled copy.
RUN CPLUS_INCLUDE_PATH=/build/_deps/absl-src cmake --build /build -j"$(nproc)"
RUN cmake --install /build \
    && ls -l /opt/arrow/lib \
    && ldd /opt/arrow/lib/libarrow_flight_sql_odbc.so \
    && ls /opt/arrow/share/*/flight/sql/odbc/

FROM ubuntu:26.04@sha256:d66b366dac61b575bee2aef07e9c26e8070096fbe84a3d4d3eb0da069b5d76db
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y -q && apt-get install -y -q --no-install-recommends \
      unixodbc odbcinst python3 python3-pyodbc ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# The driver statically links Arrow/Flight/gRPC: ship only the .so.
COPY --from=build /opt/arrow/lib/libarrow_flight_sql_odbc.so.2600.1.0 /opt/arrow/lib/libarrow_flight_sql_odbc.so
COPY --from=build /arrow/cpp/src/arrow/flight/sql/odbc/install/unix/install_odbc.sh /tmp/install_odbc.sh
# Resolve runtime deps (e.g. libssl) and register the driver via the project's script.
RUN if ldd /opt/arrow/lib/libarrow_flight_sql_odbc.so | grep "not found"; then exit 1; fi; \
    bash /tmp/install_odbc.sh /opt/arrow/lib/libarrow_flight_sql_odbc.so && rm /tmp/install_odbc.sh \
    && cat /etc/odbcinst.ini
COPY probe/odbc_probe.py /probe/odbc_probe.py
RUN python3 -m py_compile /probe/odbc_probe.py \
    && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin probe
USER 10001
WORKDIR /probe
CMD ["python3", "/probe/odbc_probe.py"]

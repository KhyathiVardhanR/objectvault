FROM ubuntu:24.04 AS build
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake git pkg-config ca-certificates \
      libboost-dev libssl-dev libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev protobuf-compiler \
      libpq-dev libhiredis-dev nlohmann-json3-dev zlib1g-dev libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*
# libpqxx 8 and prometheus-cpp are newer than the distro packages
RUN git clone --depth 1 --branch 8.0.2 https://github.com/jtv/libpqxx /src/libpqxx \
 && cmake -S /src/libpqxx -B /src/libpqxx/build -DSKIP_BUILD_TEST=ON -DBUILD_SHARED_LIBS=ON \
 && cmake --build /src/libpqxx/build -j"$(nproc)" && cmake --install /src/libpqxx/build
RUN git clone --depth 1 --branch v1.3.0 --recurse-submodules https://github.com/jupp0r/prometheus-cpp /src/prom \
 && cmake -S /src/prom -B /src/prom/build -DENABLE_PUSH=OFF -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=ON \
 && cmake --build /src/prom/build -j"$(nproc)" && cmake --install /src/prom/build
WORKDIR /src/objectvault
COPY CMakeLists.txt ./
COPY proto proto
COPY src src
COPY tools tools
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j"$(nproc)"

FROM ubuntu:24.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      libgrpc++1.51t64 libprotobuf32t64 libpq5 libhiredis1.1.0 libssl3t64 curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/lib/ /usr/local/lib/
COPY --from=build /src/objectvault/build/ov-gateway /src/objectvault/build/ov-storage /src/objectvault/build/ov-bench /usr/local/bin/
RUN ldconfig
EXPOSE 8080 9001 9200

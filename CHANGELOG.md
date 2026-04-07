# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], [markdownlint],
and this project adheres to [Semantic Versioning].

## [0.7.3] - 2026-04-07

### Changes/Additions/Fixes in version 0.7.3

- Updated `io.grpc/grpc-bom` from 1.79.0 to 1.80.0.
- Updated `com.google.protobuf/protobuf-java-util` from 4.33.6 to 4.34.1.
- Updated `com.google.protobuf/protoc` from 4.33.2 to 4.34.1.
- Updated `io.netty/netty-bom` from 4.2.10.Final to 4.2.12.Final.
- Updated `com.senzing/data-mart-replicator` from 2.0.0-beta.2.2 to 2.0.0-beta.2.4.
- Updated `com.ibm.icu/icu4j` from 78.2 to 78.3.
- Updated `com.github.spotbugs/spotbugs-maven-plugin` from 4.9.8.2 to 4.9.8.3.

## [0.7.2] - 2026-03-24

### Changes/Additions/Fixes in version 0.7.2

- Added Java code style and formatting settings (`checkstyle.xml`,
  `.vscode/java-formatter.xml`, `.vscode/settings.json`).
- Updated `checkstyle-suppressions.xml` to align with shared Senzing style
  (added `FileLength`, `NoWhitespaceAfter`; removed `LeftCurly`, `LineLength`
  suppressions now handled by `checkstyle.xml`).
- Updated `com.senzing/data-mart-replicator` dependency from 2.0.0-beta.2.1 to
  2.0.0-beta.2.2.
- Updated `com.linecorp.armeria/armeria-bom` from 1.36.0 to 1.37.0.
- Updated `com.fasterxml.jackson/jackson-bom` from 2.21.0 to 2.21.2.
- Updated `org.xerial/sqlite-jdbc` from 3.51.2.0 to 3.51.3.0.
- Updated `com.google.protobuf/protobuf-java-util` from 4.33.5 to 4.33.6.
- Updated `org.junit.jupiter/junit-jupiter` from 6.0.2 to 6.0.3.
- Updated `org.apache.maven.plugins/maven-surefire-plugin` from 3.5.4 to 3.5.5.
- Updated `org.apache.maven.plugins/maven-shade-plugin` from 3.6.1 to 3.6.2.
- Updated `org.apache.maven.plugins/maven-resources-plugin` from 3.4.0 to 3.5.0.

## [0.7.1] - 2026-03-05

### Changes/Additions/Fixes in version 0.7.1

- Fixed server JAR not being published to Maven Central by attaching it as a
  classified artifact (`server` classifier).

## [0.7.0] - 2026-03-02

### Changes/Additions/Fixes in version 0.7.0

- **Breaking:** Replaced `SzGrpcServices.getDataMartMessageQueue()` with
  `getInfoMessageConsumer()` returning `Consumer<String>`.
- **Breaking:** Replaced `SzGrpcServer.getDataMartMessageQueue()` with
  `getInfoMessageConsumer()`.
- Added new public constructor `SzGrpcServices(SzEnvironment, Consumer<String>)`
  allowing an external info message consumer to receive INFO messages without
  requiring data mart infrastructure.
- When both a data mart URI and an external info message consumer are provided
  (via the protected 4-argument constructor), both receive INFO messages.
- Updated maven central publishing to use `central-publishing-maven-plugin`
  version 0.10.0.

## [0.6.1] - 2026-02-25

### Changes/Additions/Fixes in version 0.6.1

- Fixed parsing of license expiration date to be tolerant of dates and timestamps.
- Updated dependency for `sz-sdk-auto` to version `0.5.1`
- Updated dependency for `data-mart-replicator` to version `2.0.0-beta.2.1`
- Updated dependency for `armeria-bom` to version `1.36.0`

## [0.6.0] - 2026-02-24

### Changes/Additions/Fixes in version 0.6.0

- **Breaking:** Changed `SzGrpcServices.configureServer(ServerBuilder)` to
  `configureServer(ServerBuilder, String)` — callers must now pass a data mart
  path prefix (e.g. `"/data-mart"`).
- **Breaking:** Removed `SzGrpcServices.DATA_MART_PREFIX`; use
  `SzGrpcServer.DATA_MART_PREFIX` instead.
- Made data mart path prefix configurable so callers can mount data mart
  endpoints at a custom path (e.g. `/api/senzing/data-mart`). The default
  `/data-mart` prefix is defined on `SzGrpcServer.DATA_MART_PREFIX`.

## [0.5.0] - 2026-02-24

### Changes/Additions/Fixes in version 0.5.0

- Refactored `SzGrpcServices` from `SzGrpcServer` to enable composition of the
  gRPC server in an existing Armeria server.
- Updated `data-mart-replicator` and `senzing-commons` dependencies.

## [0.4.1] - 2026-01-07

### Changes/Additions/Fixes in version 0.4.1

- Updated `com.senzing/data-mart-replicator` dependency to version 2.0.0-beta.1.4

## [0.4.0] - 2025-12-18

### Changes/Additions/Fixes in version 0.4.0

- Initial stable pre-release version.
- Adds handling core CORS `Access-Control-Allow-Origins` header.
- Updated dependencies.

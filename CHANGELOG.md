# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], [markdownlint],
and this project adheres to [Semantic Versioning].

## [0.6.0] - 2026-02-24

### Changes/Additions/Fixes in version 0.6.0

- **Breaking:** Changed `SzGrpcServices.configureServer(ServerBuilder)` to
  `configureServer(ServerBuilder, String)` — callers must now pass a data mart
  path prefix (e.g. `"/data-mart"`).
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

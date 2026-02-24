# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], [markdownlint],
and this project adheres to [Semantic Versioning].

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

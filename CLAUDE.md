# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## **IMPORTANT: Code Change Policy**

**DO NOT make direct code changes.** Instead, analyze the code, identify issues, and provide suggestions for changes. The user will review and make the changes themselves. This applies to:

- Source code modifications
- Configuration file changes
- Documentation updates (except CLAUDE.md itself when explicitly requested)

Always present recommendations clearly with explanations and let the user decide how to proceed.

### Editing CLAUDE.md

When editing this file:

- Ensure Prettier formatting is followed (blank lines before/after lists, before code blocks, etc.)
- After making changes, the file will be auto-formatted by Prettier
- Maintain proper Markdown structure with consistent spacing

## Project Overview

This is the **Senzing Java gRPC SDK** - a gRPC client and server implementation that provides remote access to Senzing entity resolution capabilities. The project consists of:

- **gRPC Client**: Java implementations of Senzing SDK interfaces that communicate over gRPC
- **gRPC Server**: Server that wraps the native Senzing Core SDK and exposes it via gRPC
- **Protocol Buffers**: Proto definitions in `sz-sdk-proto/` that define the gRPC service contracts

## Build Commands

### Basic Build

```bash
mvn clean install
```

### Build with Code Quality Profiles

```bash
# Run with checkstyle validation
mvn clean install -Pcheckstyle

# Run with spotbugs static analysis
mvn clean install -Pspotbugs

# Run with code coverage
mvn clean install -Pjacoco
```

### Running Tests

```bash
# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=EngineBasicsTest

# Run a specific test method
mvn test -Dtest=EngineBasicsTest#testPrimeEngine
```

**Important**: Tests require environment variables `SENZING_PATH` and `SENZING_DEV_LIBRARY_PATH` to be set, pointing to a Senzing installation.

### Generate Javadoc

```bash
mvn javadoc:javadoc
```

### Build Shaded JARs

The build produces two artifacts:

- `sz-sdk-grpc.jar` - Client library only (default Maven artifact)
- `sz-sdk-grpc-server.jar` - Standalone server with all dependencies (shaded)

Both are created automatically during `mvn package`.

## Code Architecture

### Client-Server Design Pattern

The codebase follows a clean separation between client and server:

**Client-side** (`com.senzing.sdk.grpc` package):

- `SzGrpcEnvironment` - Main entry point for gRPC client, manages gRPC channel and lifecycle
- `SzGrpcEngine`, `SzGrpcConfig`, `SzGrpcConfigManager`, `SzGrpcDiagnostic`, `SzGrpcProduct` - Client implementations that communicate via gRPC stubs
- Each client class wraps a gRPC blocking stub and translates SDK interface calls to gRPC requests

**Server-side** (`com.senzing.sdk.grpc.server` package):

- `SzGrpcServer` - Main server class using Armeria framework
- `SzGrpcEngineImpl`, `SzGrpcConfigImpl`, etc. - Service implementations that receive gRPC requests and delegate to native Senzing Core SDK
- `WrapperMain` - Entry point for standalone server execution
- Server implementations translate gRPC requests to Core SDK calls and handle exception mapping

### Key Architectural Patterns

**Environment Lifecycle Management**:

- `SzGrpcEnvironment` uses a state machine (`ACTIVE`, `DESTROYING`, `DESTROYED`) with read-write locks
- The `execute()` method ensures operations complete before destruction
- Client never destroys the gRPC channel - it's externally managed

**Exception Mapping**:

- Server creates structured JSON error messages with error codes, reasons, stack traces
- Client parses these JSON errors back into appropriate `SzException` subclasses
- See `SzGrpcEnvironment.createSzException()` and `SzGrpcServer.toStatusRuntimeException()`

**Streaming Support**:

- Export operations use server-side streaming (`StreamExportJsonEntityReport`, `StreamExportCsvEntityReport`)
- Client maintains export handle mappings to stream iterators
- Fibonacci sequence used for export handles to avoid collisions

**Builder Pattern**:

- `SzGrpcEnvironment.Builder` for client initialization
- `SzGrpcServerOptions` for server configuration with command-line parsing support

### Generated Code

**Protocol Buffers** are compiled during build:

- Proto files in `sz-sdk-proto/` are compiled by `protobuf-maven-plugin`
- Generated code goes to `target/generated-sources/protobuf/`
- Maven replacer plugin adds Javadoc to generated gRPC service classes
- `InstallUtilities.java` is copied from `sz-sdk-java` dependency and repackaged

### Test Structure

Tests inherit from `AbstractGrpcTest` which:

- Starts a gRPC server before tests
- Provides shared test data and utilities
- Manages server lifecycle
- Tests are organized by SDK interface: `ConfigTest`, `EngineBasicsTest`, `EngineGraphTest`, etc.

## Development Guidelines

### Dependencies and Shading

The server JAR uses extensive dependency shading to avoid classpath conflicts:

- All third-party packages relocated to `com.senzing.sdk.grpc.shaded.*`
- Exception: `sz-sdk` interfaces are NOT shaded (required for API compatibility)
- Netty native transports are excluded (using NIO transport via `com.linecorp.armeria.transportType=nio`)

### When Adding New gRPC Methods

1. Update proto file in `sz-sdk-proto/`
2. Run `mvn generate-sources` to regenerate gRPC stubs
3. Implement server-side handler in appropriate `*Impl` class
4. Implement client-side method in corresponding gRPC client class
5. Map request/response objects and handle exceptions appropriately

### Exception Handling Convention

Server-side exception handling in `SzGrpcServer`:

- Map Java exception types to gRPC `Status` codes via `STATUS_MAP`
- For `SzException`, preserve error code in reason string as `SENZ<code>|<message>`
- Include stack trace and function information in JSON structure

Client-side parsing in `SzGrpcEnvironment`:

- Extract error code from reason prefix
- Parse JSON error structure for additional context
- Reconstruct appropriate `SzException` subclass using `SzCoreUtilities.createSzException()`

### Checkstyle Suppressions

Suppressions are defined in `checkstyle-suppressions.xml`. Common suppressions for generated code and specific patterns.

### SpotBugs Exclusions

Security and bug pattern exclusions are in `spotbugs-exclude.xml`.

## Running the Server

```bash
# Run the shaded server JAR
java -jar target/sz-sdk-grpc-server.jar \
  --ini-file <path-to-senzing-config> \
  --port 8261 \
  --concurrency 10

# See all available options
java -jar target/sz-sdk-grpc-server.jar --help
```

Key server options (from `SzGrpcServerOption`):

- `--ini-file` / `--settings` - Senzing configuration
- `--port` - gRPC server port (default: 8261)
- `--bind-address` - Network interface to bind
- `--concurrency` - Thread pool size for request handling
- `--grpc-concurrency` - gRPC executor concurrency

## Project Structure Notes

- `sz-sdk-java/` - Included as Git submodule, provides shared tests and utilities
- `sz-sdk-proto/` - Proto definitions (shared across language implementations)
- Tests use `target/java-wrapper/bin/java-wrapper.bat` - a generated wrapper that sets up Senzing native library paths
- Demo code in `src/demo/java/` is included in test classpath for Javadoc snippets (Java 18+)

## Maven Profiles

- `release` - Enables GPG signing for deployment
- `jacoco` - Code coverage reporting
- `checkstyle` - Style validation
- `spotbugs` - Static analysis with security checks (findsecbugs)
- `java-17` / `java-18+` - Java version-specific configurations for Javadoc

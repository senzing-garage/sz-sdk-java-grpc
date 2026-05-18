# sz-sdk-java-grpc

The Senzing Java gRPC SDK provides a gRPC client and server for remote
access to Senzing entity resolution capabilities.

## Overview

This project provides two ways to use Senzing over gRPC:

- **Standalone server** (`SzGrpcServer`) — A self-contained Armeria-based
  gRPC server that manages the full lifecycle: Senzing environment
  initialization, gRPC service registration, and shutdown.

- **Composable services** (`SzGrpcServices`) — A composable unit that
  can be embedded into an existing Armeria `ServerBuilder`, allowing
  Senzing gRPC services to run alongside other services in a single
  server.

## Usage

### Standalone Server

Run the shaded server JAR:

```bash
java -jar target/sz-sdk-grpc-server.jar \
  --ini-file <path-to-senzing-config> \
  --port 8261 \
  --concurrency 10
```

Use `--help` for all available options.

### Embedding into an Existing Armeria Server

Use `SzGrpcServices` to add Senzing gRPC endpoints to your own Armeria
server:

```java
SzEnvironment env = ... ; // your existing SzEnvironment

SzGrpcServices services = new SzGrpcServices(env);

ServerBuilder builder = Server.builder()
    .http(8261)
    .blockingTaskExecutor(10);

services.configureServer(builder, "/data-mart");

Server server = builder.build();
server.start().join();
services.start();

// ... when shutting down:
server.stop().join();
services.destroy();
```

**Note:** If Netty native transports (Epoll, io_uring) are not on the
classpath, you must set `com.linecorp.armeria.transportType` to `"nio"`
in a static initializer before any Armeria class is loaded.
`SzGrpcServer` handles this automatically; when using `SzGrpcServices`
directly, this is the caller's responsibility.

## Build

```bash
# Build with tests
mvn clean install

# Build with code quality checks
mvn clean install -Pcheckstyle,spotbugs,jacoco
```

Tests require `SENZING_PATH` and `SENZING_DEV_LIBRARY_PATH` environment
variables pointing to a Senzing installation.

### Private submodule dependency

> **Note:** Although this repository is public, it depends on the
> `sz-sdk-java` Git submodule which is hosted as a **private** GitHub
> repository (used to obtain shared test classes and the
> `InstallUtilities` source that this project repackages). As a result,
> recursive clones and `mvn clean install` builds will fail for anyone
> without read access to the `senzing-garage/sz-sdk-java` repository.
>
> If you do not have access, contact Senzing for credentials. The
> recursive clone is typically performed via:
>
> ```bash
> git clone --recurse-submodules <repo-url>
> # or, after a non-recursive clone:
> git submodule update --init --recursive
> ```

## Artifacts

The build produces two JARs:

- `sz-sdk-grpc.jar` — Client library only (default Maven artifact)
- `sz-sdk-grpc-server.jar` — Standalone server with all dependencies (shaded)

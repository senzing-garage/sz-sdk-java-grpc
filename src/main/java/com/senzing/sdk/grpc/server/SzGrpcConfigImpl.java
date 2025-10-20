package com.senzing.sdk.grpc.server;

import java.util.Objects;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEnvironment;
import io.grpc.stub.StreamObserver;

import static com.senzing.sdk.grpc.proto.SzConfigGrpc.*;
import static com.senzing.sdk.grpc.proto.SzConfigProto.*;
import static com.senzing.sdk.grpc.server.SzGrpcServer.*;

/**
 * Provides the gRPC server-side implementation for {@link SzConfig}.
 */
public class SzGrpcConfigImpl extends SzConfigImplBase {
    /**
     * The {@link SzGrpcServer} to use.
     */
    SzGrpcServer server = null;

    /**
     * Constructs with the {@link SzGrpcServer}.
     * 
     * @param server The {@link SzGrpcServer}.
     */
    public SzGrpcConfigImpl(SzGrpcServer server) {
        Objects.requireNonNull(server, "The server cannot be null");
        if (server.isDestroyed()) {
            throw new IllegalArgumentException(
                "The specified SzCoreEnvironment has already been destroyed");
        }
        this.server = server;
    }

    /**
     * Gets the {@link SzEnvironment} to use from the backing
     * {@link SzGrpcServer}.
     * 
     * @return The {@link SzEnvironment} for the backing server.
     */
    private SzEnvironment getEnvironment() {
        return this.server.getEnvironment();
    }

    @Override
    public void getDataSourceRegistry(
            GetDataSourceRegistryRequest                    request,
            StreamObserver<GetDataSourceRegistryResponse>   responseObserver) 
    {
        try {
            String configDef = request.getConfigDefinition();

            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            SzConfig config = configMgr.createConfig(configDef);

            String result = config.getDataSourceRegistry();

            GetDataSourceRegistryResponse response
                = GetDataSourceRegistryResponse.newBuilder()
                    .setResult(result).build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    @Override
    public void registerDataSource(
            RegisterDataSourceRequest                   request,
            StreamObserver<RegisterDataSourceResponse>  responseObserver) 
    {
        try {
            String configDef    = request.getConfigDefinition();
            String dataSource   = request.getDataSourceCode();

            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            SzConfig config = configMgr.createConfig(configDef);

            String result = config.registerDataSource(dataSource);

            configDef = config.export();

            RegisterDataSourceResponse response
                = RegisterDataSourceResponse.newBuilder()
                    .setConfigDefinition(configDef)
                    .setResult(result).build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    @Override
    public void unregisterDataSource(
            UnregisterDataSourceRequest                     request,
            StreamObserver<UnregisterDataSourceResponse>    responseObserver) 
    {
        try {
            String configDef    = request.getConfigDefinition();
            String dataSource   = request.getDataSourceCode();

            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            SzConfig config = configMgr.createConfig(configDef);

            config.unregisterDataSource(dataSource);

            configDef = config.export();

            UnregisterDataSourceResponse response
                = UnregisterDataSourceResponse.newBuilder()
                    .setConfigDefinition(configDef).build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    @Override
    public void verifyConfig(
        VerifyConfigRequest                     request, 
        StreamObserver<VerifyConfigResponse>    responseObserver) 
    {
        try {
            String configDef    = request.getConfigDefinition();

            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            configMgr.createConfig(configDef);

            VerifyConfigResponse response
                = VerifyConfigResponse.newBuilder()
                    .setResult(true).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }
    
}

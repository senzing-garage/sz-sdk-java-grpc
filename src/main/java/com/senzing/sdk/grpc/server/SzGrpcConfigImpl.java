package com.senzing.sdk.grpc.server;

import java.util.Objects;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEnvironment;

import io.grpc.stub.StreamObserver;

import static com.senzing.sdk.grpc.proto.SzConfigGrpc.*;
import static com.senzing.sdk.grpc.proto.SzConfigProto.*;
import static com.senzing.sdk.grpc.server.SzGrpcServices.*;

/**
 * Provides the gRPC server-side implementation for {@link SzConfig}.
 */
public class SzGrpcConfigImpl extends SzConfigImplBase {
    /**
     * The {@link SzGrpcServices} to use.
     */
    private SzGrpcServices services = null;

    /**
     * Constructs with the {@link SzGrpcServices}.
     *
     * @param services The {@link SzGrpcServices}.
     */
    protected SzGrpcConfigImpl(SzGrpcServices services) {
        Objects.requireNonNull(services, "The services cannot be null");
        if (services.isDestroyed()) {
            throw new IllegalArgumentException(
                "The specified SzGrpcServices has already been destroyed");
        }
        this.services = services;
    }

    /**
     * Gets the {@link SzEnvironment} to use from the backing
     * {@link SzGrpcServices}.
     *
     * @return The {@link SzEnvironment} for the backing services.
     */
    protected SzEnvironment getEnvironment() {
        return this.services.getEnvironment();
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzConfig#getDataSourceRegistry()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzConfig#registerDataSource(String)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzConfig#unregisterDataSource(String)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzConfigManager#createConfig(String)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

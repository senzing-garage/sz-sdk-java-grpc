package com.senzing.sdk.grpc.server;

import java.util.Objects;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.grpc.server.SzConfigManagerGrpc.SzConfigManagerImplBase;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.GetConfigRegistryRequest;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.GetConfigRegistryResponse;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.GetConfigRequest;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.GetConfigResponse;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.GetDefaultConfigIdRequest;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.GetDefaultConfigIdResponse;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.GetTemplateConfigRequest;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.GetTemplateConfigResponse;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.RegisterConfigRequest;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.RegisterConfigResponse;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.ReplaceDefaultConfigIdRequest;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.ReplaceDefaultConfigIdResponse;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.SetDefaultConfigIdRequest;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.SetDefaultConfigIdResponse;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.SetDefaultConfigRequest;
import com.senzing.sdk.grpc.server.SzConfigManagerProto.SetDefaultConfigResponse;

import io.grpc.stub.StreamObserver;

/**
 * Provides the gRPC server-side implementation for {@link SzConfigManager}.
 */
public class SzGrpcConfigManagerImpl extends SzConfigManagerImplBase {
    /**
     * The {@link SzGrpcServer} to use.
     */
    SzGrpcServer server = null;

    /**
     * Constructs with the {@link SzGrpcServer}.
     * 
     * @param server The {@link SzGrpcServer}.
     */
    public SzGrpcConfigManagerImpl(SzGrpcServer server) {
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
    public void getConfig(GetConfigRequest                  request, 
                          StreamObserver<GetConfigResponse> responseObserver) 
    {
        try {
            long configId = request.getConfigId();

            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            SzConfig config = configMgr.createConfig(configId);

            String result = config.export();

            GetConfigResponse response
                = GetConfigResponse.newBuilder()
                    .setResult(result).build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getConfigRegistry(
        GetConfigRegistryRequest                    request,
        StreamObserver<GetConfigRegistryResponse>   responseObserver) 
    {
        try {
            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            String result = configMgr.getConfigRegistry();

            GetConfigRegistryResponse response
                = GetConfigRegistryResponse.newBuilder()
                    .setResult(result).build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getDefaultConfigId(
            GetDefaultConfigIdRequest                   request,
            StreamObserver<GetDefaultConfigIdResponse>  responseObserver) 
    {
        try {
            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            long result = configMgr.getDefaultConfigId();

            GetDefaultConfigIdResponse response
                = GetDefaultConfigIdResponse.newBuilder()
                    .setResult(result).build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getTemplateConfig(
            GetTemplateConfigRequest                    request,
            StreamObserver<GetTemplateConfigResponse>   responseObserver) 
    {
        try {
            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            SzConfig config = configMgr.createConfig();

            String result = config.export();

            GetTemplateConfigResponse response
                = GetTemplateConfigResponse.newBuilder()
                    .setResult(result).build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void registerConfig(
        RegisterConfigRequest                   request, 
        StreamObserver<RegisterConfigResponse>  responseObserver) 
    {
        try {
            String configDef        = request.getConfigDefinition();
            String configComment    = request.getConfigComment();

            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            long result = configMgr.registerConfig(configDef, configComment);
            
            RegisterConfigResponse response
                = RegisterConfigResponse.newBuilder()
                    .setResult(result).build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void replaceDefaultConfigId(
            ReplaceDefaultConfigIdRequest                   request,
            StreamObserver<ReplaceDefaultConfigIdResponse>  responseObserver) 
    {
        try {
            long currentConfigId    = request.getCurrentDefaultConfigId();
            long newConfigId        = request.getNewDefaultConfigId();
            
            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            configMgr.replaceDefaultConfigId(currentConfigId, newConfigId);
            
            ReplaceDefaultConfigIdResponse response
                = ReplaceDefaultConfigIdResponse.newBuilder().build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void setDefaultConfig(
            SetDefaultConfigRequest                     request,
            StreamObserver<SetDefaultConfigResponse>    responseObserver) 
    {
        try {
            String configDef        = request.getConfigDefinition();
            String configComment    = request.getConfigComment();

            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            long result = configMgr.registerConfig(configDef, configComment);
            
            configMgr.setDefaultConfigId(result);
            
            SetDefaultConfigResponse response
                = SetDefaultConfigResponse.newBuilder()
                    .setResult(result).build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void setDefaultConfigId(
            SetDefaultConfigIdRequest                   request,
            StreamObserver<SetDefaultConfigIdResponse>  responseObserver) 
    {
        try {
            long configId = request.getConfigId();

            SzConfigManager configMgr = this.getEnvironment().getConfigManager();

            configMgr.setDefaultConfigId(configId);
            
            SetDefaultConfigIdResponse response
                = SetDefaultConfigIdResponse.newBuilder().build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

}

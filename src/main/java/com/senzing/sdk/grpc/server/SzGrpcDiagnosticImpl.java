package com.senzing.sdk.grpc.server;

import java.util.Objects;

import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzEnvironment;
import io.grpc.stub.StreamObserver;

import static com.senzing.sdk.grpc.proto.SzDiagnosticGrpc.*;
import static com.senzing.sdk.grpc.proto.SzDiagnosticProto.*;
import static com.senzing.sdk.grpc.server.SzGrpcServer.*;

/**
 * Provides the gRPC server-side implementation for {@link SzDiagnostic}.
 */
class SzGrpcDiagnosticImpl extends SzDiagnosticImplBase {
    /**
     * The {@link SzGrpcServer} to use.
     */
    SzGrpcServer server = null;

    /**
     * Constructs with the {@link SzGrpcServer}.
     * 
     * @param server The {@link SzGrpcServer}.
     */
    public SzGrpcDiagnosticImpl(SzGrpcServer server) {
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
    public void checkRepositoryPerformance(
            CheckRepositoryPerformanceRequest                   request,
            StreamObserver<CheckRepositoryPerformanceResponse>  responseObserver) 
    {
        try {
            int secondsToRun = request.getSecondsToRun();
        
            SzDiagnostic diagnostic = this.getEnvironment().getDiagnostic();

            String result = diagnostic.checkRepositoryPerformance(secondsToRun);

            CheckRepositoryPerformanceResponse response
                = CheckRepositoryPerformanceResponse.newBuilder()
                    .setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    @Override
    public void getFeature(
            GetFeatureRequest                   request, 
            StreamObserver<GetFeatureResponse>  responseObserver) 
    {
        try {
            long featureId = request.getFeatureId();
        
            SzDiagnostic diagnostic = this.getEnvironment().getDiagnostic();

            String result = diagnostic.getFeature(featureId);

            GetFeatureResponse response
                = GetFeatureResponse.newBuilder()
                    .setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    @Override
    public void getRepositoryInfo(
            GetRepositoryInfoRequest                    request,
            StreamObserver<GetRepositoryInfoResponse>   responseObserver) 
    {
        try {
            SzDiagnostic diagnostic = this.getEnvironment().getDiagnostic();

            String result = diagnostic.getRepositoryInfo();

            GetRepositoryInfoResponse response
                = GetRepositoryInfoResponse.newBuilder()
                    .setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    @Override
    public void purgeRepository(
            PurgeRepositoryRequest                  request,
            StreamObserver<PurgeRepositoryResponse> responseObserver) 
    {
        try {
            SzDiagnostic diagnostic = this.getEnvironment().getDiagnostic();

            diagnostic.purgeRepository();

            PurgeRepositoryResponse response
                = PurgeRepositoryResponse.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    @Override
    public void reinitialize(
            ReinitializeRequest                     request, 
            StreamObserver<ReinitializeResponse>    responseObserver) 
    {
        try {
            long configId = request.getConfigId();

            this.getEnvironment().reinitialize(configId);

            ReinitializeResponse response
                = ReinitializeResponse.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    
}

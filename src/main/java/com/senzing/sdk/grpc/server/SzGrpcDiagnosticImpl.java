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
public class SzGrpcDiagnosticImpl extends SzDiagnosticImplBase {
    /**
     * The {@link SzGrpcServer} to use.
     */
    private SzGrpcServer server = null;

    /**
     * Constructs with the {@link SzGrpcServer}.
     * 
     * @param server The {@link SzGrpcServer}.
     */
    protected SzGrpcDiagnosticImpl(SzGrpcServer server) {
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
    protected SzEnvironment getEnvironment() {
        return this.server.getEnvironment();
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzDiagnostic#checkRepositoryPerformance(int)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzDiagnostic#getFeature(long)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzDiagnostic#getRepositoryInfo()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzDiagnostic#purgeRepository()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEnvironment#reinitialize(long)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

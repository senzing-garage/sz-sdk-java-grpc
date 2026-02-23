package com.senzing.sdk.grpc.server;

import java.util.Objects;

import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzEnvironment;

import io.grpc.stub.StreamObserver;

import static com.senzing.sdk.grpc.proto.SzDiagnosticGrpc.*;
import static com.senzing.sdk.grpc.proto.SzDiagnosticProto.*;
import static com.senzing.sdk.grpc.server.SzGrpcServices.*;

/**
 * Provides the gRPC server-side implementation for {@link SzDiagnostic}.
 */
public class SzGrpcDiagnosticImpl extends SzDiagnosticImplBase {
    /**
     * The {@link SzGrpcServices} to use.
     */
    private SzGrpcServices services = null;

    /**
     * Constructs with the {@link SzGrpcServices}.
     *
     * @param services The {@link SzGrpcServices}.
     */
    protected SzGrpcDiagnosticImpl(SzGrpcServices services) {
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

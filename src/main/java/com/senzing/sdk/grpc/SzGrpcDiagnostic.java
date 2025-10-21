package com.senzing.sdk.grpc;

import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzException;

import com.senzing.sdk.grpc.proto.SzDiagnosticGrpc;

import static com.senzing.sdk.grpc.proto.SzDiagnosticGrpc.*;
import static com.senzing.sdk.grpc.proto.SzDiagnosticProto.*;

import io.grpc.Channel;

/**
 * The gRPC implementation of {@link SzDiagnostic}.
 */
public class SzGrpcDiagnostic implements SzDiagnostic {
    /**
     * The {@link SzGrpcEnvironment} that constructed this instance.
     */
    private SzGrpcEnvironment env = null;

    /**
     * The underlying blocking stub.
     */
    private SzDiagnosticBlockingStub blockingStub = null;

    /**
     * Package-access constructor.
     * 
     * @param environment the {@link SzGrpcEnvironment} with which to construct.
     */
    SzGrpcDiagnostic(SzGrpcEnvironment environment) {
        this.env = environment;
        
        Channel channel = this.env.getChannel();

        this.blockingStub = SzDiagnosticGrpc.newBlockingStub(channel);
    }

    /**
     * Gets the underlying {@link SzDiagnosticBlockingStub} for this instance.
     * 
     * @return The underlying {@link SzDiagnosticBlockingStub} for this instance.
     */
    protected SzDiagnosticBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String getRepositoryInfo() throws SzException {
        return this.env.execute(() -> {

            GetRepositoryInfoRequest request
                = GetRepositoryInfoRequest.newBuilder().build();
            
            GetRepositoryInfoResponse response
                = this.getBlockingStub().getRepositoryInfo(request);

            return response.getResult();
        });      
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String checkRepositoryPerformance(int secondsToRun) throws SzException {
        return this.env.execute(() -> {

            CheckRepositoryPerformanceRequest request
                = CheckRepositoryPerformanceRequest.newBuilder()
                    .setSecondsToRun(secondsToRun).build();
            
            CheckRepositoryPerformanceResponse response
                = this.getBlockingStub().checkRepositoryPerformance(request);

            return response.getResult();
        });      
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}
     * (assuming the gRPC allows the operation).
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void purgeRepository() throws SzException {
        this.env.execute(() -> {

            PurgeRepositoryRequest request
                = PurgeRepositoryRequest.newBuilder().build();
            
            this.getBlockingStub().purgeRepository(request);

            return null;
        });      
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String getFeature(long featureId) throws SzException {
        return this.env.execute(() -> {

            GetFeatureRequest request
                = GetFeatureRequest.newBuilder()
                    .setFeatureId(featureId).build();
            
            GetFeatureResponse response
                = this.getBlockingStub().getFeature(request);

            return response.getResult();
        });      
    }
    
}

package com.senzing.sdk.grpc;

import com.senzing.sdk.SzException;
import com.senzing.sdk.SzProduct;

import com.senzing.sdk.grpc.proto.SzProductGrpc;

import static com.senzing.sdk.grpc.proto.SzProductGrpc.*;
import static com.senzing.sdk.grpc.proto.SzProductProto.*;

import io.grpc.Channel;

/**
 * The gRPC implementation of {@link SzProduct}.
 */
public class SzGrpcProduct implements SzProduct {
    /**
     * The {@link SzGrpcEnvironment} that constructed this instance.
     */
    private SzGrpcEnvironment env = null;

    /**
     * The underlying blocking stub.
     */
    private SzProductBlockingStub blockingStub = null;

    /**
     * Package-access constructor.
     * 
     * @param environment the {@link SzGrpcEnvironment} with which to construct.
     */
    protected SzGrpcProduct(SzGrpcEnvironment environment) {
        this.env = environment;
        
        Channel channel = this.env.getChannel();

        this.blockingStub = SzProductGrpc.newBlockingStub(channel);
    }

    /**
     * Gets the underlying {@link SzProductBlockingStub} for this instance.
     * 
     * @return The underlying {@link SzProductBlockingStub} for this instance.
     */
    SzProductBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String getLicense() throws SzException {
        return this.env.execute(() -> {

            GetLicenseRequest request
                = GetLicenseRequest.newBuilder().build();
            
            GetLicenseResponse response
                = this.getBlockingStub().getLicense(request);

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
    public String getVersion() throws SzException {
        return this.env.execute(() -> {

            GetVersionRequest request
                = GetVersionRequest.newBuilder().build();
            
            GetVersionResponse response
                = this.getBlockingStub().getVersion(request);

            return response.getResult();
        });      
    }
    
}

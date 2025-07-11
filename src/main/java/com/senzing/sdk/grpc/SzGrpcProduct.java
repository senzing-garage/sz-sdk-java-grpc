package com.senzing.sdk.grpc;

import com.senzing.sdk.SzException;
import com.senzing.sdk.SzProduct;
import com.senzing.sdk.grpc.SzProductGrpc.SzProductBlockingStub;

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
    SzGrpcProduct(SzGrpcEnvironment environment) {
        this.env = environment;
        
        Channel channel = this.env.getChannel();

        this.blockingStub = SzProductGrpc.newBlockingStub(channel);
    }

    /**
     * Gets the underlying {@link SzProductBlockingStub} for this instance.
     */
    SzProductBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    @Override
    public String getLicense() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLicense'");
    }

    @Override
    public String getVersion() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getVersion'");
    }
    
}

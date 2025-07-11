package com.senzing.sdk.grpc;

import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzException;
import com.senzing.sdk.grpc.SzDiagnosticGrpc.SzDiagnosticBlockingStub;

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
     */
    SzDiagnosticBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    @Override
    public String getRepositoryInfo() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDatastoreInfo'");
    }

    @Override
    public String checkRepositoryPerformance(int secondsToRun) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkDatastorePerformance'");
    }

    @Override
    public void purgeRepository() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'purgeRepository'");
    }

    @Override
    public String getFeature(long featureId) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFeature'");
    }
    
}

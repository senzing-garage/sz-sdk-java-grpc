package com.senzing.sdk.grpc;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzReplaceConflictException;
import com.senzing.sdk.grpc.SzConfigManagerGrpc.SzConfigManagerBlockingStub;
import com.senzing.sdk.grpc.SzConfigGrpc.SzConfigBlockingStub;

import io.grpc.Channel;

/**
 * The gRPC implementation of {@link SzConfigManager}. 
 */
public class SzGrpcConfigManager implements SzConfigManager {
    /**
     * The {@link SzGrpcEnvironment} that constructed this instance.
     */
    private SzGrpcEnvironment env = null;

    /**
     * The underlying blocking stub.
     */
    private SzConfigManagerBlockingStub blockingStub = null;

    /**
     * The underlying "config" blocking stub.
     */
    private SzConfigBlockingStub configBlockingStub = null;

    /**
     * Package-access constructor.
     * 
     * @param environment the {@link SzGrpcEnvironment} with which to construct.
     */
    SzGrpcConfigManager(SzGrpcEnvironment environment) {
        this.env = environment;
        
        Channel channel = this.env.getChannel();

        this.blockingStub = SzConfigManagerGrpc.newBlockingStub(channel);
        this.configBlockingStub = SzConfigGrpc.newBlockingStub(channel);
    }

    /**
     * Gets the underlying {@link SzConfigManagerBlockingStub} for this instance.
     */
    SzConfigManagerBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    /**
     * Gets the underlying {@link SzConfigBlockingStub} for this instance.
     */
    SzConfigBlockingStub getConfigBlockingStub() {
        return this.configBlockingStub;
    }

    @Override
    public SzConfig createConfig() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createConfig'");
    }

    @Override
    public SzConfig createConfig(String configDefinition) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createConfig'");
    }

    @Override
    public SzConfig createConfig(long configId) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createConfig'");
    }

    @Override
    public long registerConfig(String configDefinition, String configComment) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'registerConfig'");
    }

    @Override
    public long registerConfig(String configDefinition) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'registerConfig'");
    }

    @Override
    public String getConfigRegistry() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfigs'");
    }

    @Override
    public long getDefaultConfigId() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDefaultConfigId'");
    }

    @Override
    public void replaceDefaultConfigId(long currentDefaultConfigId, long newDefaultConfigId)
            throws SzReplaceConflictException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'replaceDefaultConfigId'");
    }

    @Override
    public void setDefaultConfigId(long configId) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setDefaultConfigId'");
    }

    @Override
    public long setDefaultConfig(String configDefinition, String configComment) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setDefaultConfig'");
    }

    @Override
    public long setDefaultConfig(String configDefinition) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setDefaultConfig'");
    }
    
}

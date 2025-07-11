package com.senzing.sdk.grpc;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.grpc.SzConfigGrpc.SzConfigBlockingStub;

/**
 * The gRPC implementation of {@link SzConfig}.
 */
public class SzGrpcConfig implements SzConfig {
    /**
     * The {@link SzGrpcEnvironment} that constructed this instance.
     */
    private SzGrpcEnvironment env = null;

    /**
     * The underlying blocking stub.
     */
    private SzConfigBlockingStub blockingStub = null;

    /**
     * The backing config definition.
     */
    private String configDefinition = null;

    /**
     * Package-access constructor.
     * 
     * @param environment the {@link SzGrpcEnvironment} with which to construct.
     * 
     * @param configDefinition The {@link String} config definition describing the
     *                         configuration represented by this instance.
     * 
     * @throws IllegalStateException If the underlying {@link SzCoreEnvironment} instance 
     *                               has already been destroyed.
     * 
     * @throws SzException If a Senzing failure occurs during initialization.
     */
    SzGrpcConfig(SzGrpcEnvironment environment, String configDefinition) 
        throws IllegalStateException, SzException
    {
        if (configDefinition == null) {
            throw new NullPointerException(
                "The specified config definition cannot be null");
        }

        this.env = environment;
        this.configDefinition = configDefinition;
        
        SzGrpcConfigManager configMgr 
            = (SzGrpcConfigManager) this.env.getConfigManager();

        this.blockingStub = configMgr.getConfigBlockingStub();
    }

    @Override
    public String export() throws SzException {
        return this.configDefinition;
    }

    @Override
    public String getDataSourceRegistry() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDataSources'");
    }

    @Override
    public String registerDataSource(String dataSourceCode) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addDataSource'");
    }

    @Override
    public void unregisterDataSource(String dataSourceCode) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteDataSource'");
    }
    
}

package com.senzing.sdk.grpc;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.SzCoreEnvironment;
import static com.senzing.sdk.grpc.SzConfigGrpc.*;
import static com.senzing.sdk.grpc.SzConfigProto.*;

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

    /**
     * Gets the underlying {@link SzConfigBlockingStub} for this instance.
     * 
     * @return The {@link SzConfigBlockingStub} for this instance.
     */
    SzConfigBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    @Override
    public String export() throws SzException {
        return this.configDefinition;
    }

    @Override
    public String getDataSourceRegistry() throws SzException {
        return this.env.execute(() -> {
            GetDataSourceRegistryRequest request
                = GetDataSourceRegistryRequest.newBuilder()
                    .setConfigDefinition(this.configDefinition).build();
            
            GetDataSourceRegistryResponse response
                = this.getBlockingStub().getDataSourceRegistry(request);

            return response.getResult();
        });      
    }

    @Override
    public String registerDataSource(String dataSourceCode) throws SzException 
    {
        return this.env.execute(() -> {
            RegisterDataSourceRequest request
                = RegisterDataSourceRequest.newBuilder()
                    .setDataSourceCode(dataSourceCode)
                    .setConfigDefinition(this.configDefinition).build();
            
            RegisterDataSourceResponse response
                = this.getBlockingStub().registerDataSource(request);

            // update the config definition
            this.configDefinition = response.getConfigDefinition();

            // return the result
            return response.getResult();
        });      
    }

    @Override
    public void unregisterDataSource(String dataSourceCode) throws SzException {
        this.env.execute(() -> {
            UnregisterDataSourceRequest request
                = UnregisterDataSourceRequest.newBuilder()
                    .setDataSourceCode(dataSourceCode)
                    .setConfigDefinition(this.configDefinition).build();
            
            UnregisterDataSourceResponse response
                = this.getBlockingStub().unregisterDataSource(request);

            // update the config definition
            this.configDefinition = response.getConfigDefinition();

            // return the result
            return response.getResult();
        });      
    }
    
}

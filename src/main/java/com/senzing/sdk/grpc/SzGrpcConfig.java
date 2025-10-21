package com.senzing.sdk.grpc;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzEnvironmentDestroyedException;
import com.senzing.sdk.SzException;
import com.senzing.sdk.grpc.proto.SzConfigGrpc.SzConfigBlockingStub;
import com.senzing.sdk.grpc.proto.SzConfigProto.GetDataSourceRegistryRequest;
import com.senzing.sdk.grpc.proto.SzConfigProto.GetDataSourceRegistryResponse;
import com.senzing.sdk.grpc.proto.SzConfigProto.RegisterDataSourceRequest;
import com.senzing.sdk.grpc.proto.SzConfigProto.RegisterDataSourceResponse;
import com.senzing.sdk.grpc.proto.SzConfigProto.UnregisterDataSourceRequest;
import com.senzing.sdk.grpc.proto.SzConfigProto.UnregisterDataSourceResponse;

import static com.senzing.sdk.grpc.proto.SzConfigGrpc.*;
import static com.senzing.sdk.grpc.proto.SzConfigProto.*;

import java.util.Objects;

/**
 * The gRPC implementation of {@link SzConfig}.
 */
public class SzGrpcConfig implements SzConfig {
    /**
     * The result from the {@link #toString()} function if the environment
     * is already destroyed.
     */
    static final String DESTROYED_MESSAGE = "*** DESTROYED ***";

    /**
     * The prefix to use if an {@link SzException} is thrown from 
     * {@link #export()} and {@link #toString()} was called.
     */
    static final String FAILURE_PREFIX = "*** FAILURE: ";

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
     * @throws IllegalStateException If the specified {@link SzGrpcEnvironment}
     *                               instance has already been destroyed.
     * 
     * @throws SzException If a Senzing failure occurs during initialization.
     */
    protected SzGrpcConfig(SzGrpcEnvironment environment, String configDefinition) 
        throws IllegalStateException, SzException
    {
        Objects.requireNonNull(configDefinition, "The specified config definition cannot be null");

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
    protected SzConfigBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String export() throws SzException {
        return this.configDefinition;
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
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

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
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

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
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
    
    /**
     * Implemented to execute to attempt to the operation via 
     * {@link #export()} and if that fails to return a default message
     * describing the failure instead of the config definition.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        try {
            return this.export();
        } catch (SzEnvironmentDestroyedException e) {
            return DESTROYED_MESSAGE;
        } catch (Exception e) {
            return FAILURE_PREFIX + e.getMessage();
        }
    }
}

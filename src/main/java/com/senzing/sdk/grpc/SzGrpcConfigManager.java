package com.senzing.sdk.grpc;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzConfigurationException;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzReplaceConflictException;

import static com.senzing.sdk.grpc.SzConfigManagerGrpc.*;
import static com.senzing.sdk.grpc.SzConfigManagerProto.*;
import static com.senzing.sdk.grpc.SzConfigProto.*;
import static com.senzing.sdk.grpc.SzConfigGrpc.*;

import io.grpc.Channel;

import static com.senzing.sdk.core.SzCoreUtilities.createConfigComment;

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
     * 
     * @return The underlying {@link SzConfigManagerBlockingStub} for this instance.
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
        return this.env.execute(() -> {
            GetTemplateConfigRequest request
                = GetTemplateConfigRequest.newBuilder().build();
            
            GetTemplateConfigResponse response
                = this.getBlockingStub().getTemplateConfig(request);

            String configDef = response.getResult();

            return new SzGrpcConfig(this.env, configDef);
        });      
    }

    @Override
    public SzConfig createConfig(String configDefinition)
        throws SzException 
    {
        return this.env.execute(() -> {
            VerifyConfigRequest request
                = VerifyConfigRequest.newBuilder().build();
            
            VerifyConfigResponse response
                = this.getConfigBlockingStub().verifyConfig(request);

            boolean validConfig = response.getResult();

            // we should not get here if the config is invalid since we should
            // get an exception when calling verifyConfig() or calling getResult()
            if (!validConfig) {
                throw new SzConfigurationException("Configuration is not valid");
            }
            
            return new SzGrpcConfig(this.env, configDefinition);
        });      
    }

    @Override
    public SzConfig createConfig(long configId)
        throws SzException
    {
        return this.env.execute(() -> {
            GetConfigRequest request
                = GetConfigRequest.newBuilder()
                    .setConfigId(configId).build();
            
            GetConfigResponse response
                = this.getBlockingStub().getConfig(request);

            String configDef = response.getResult();

            return new SzGrpcConfig(this.env, configDef);
        });      
    }

    @Override
    public long registerConfig(String configDefinition, String configComment) 
        throws SzException 
    {
        return this.env.execute(() -> {
            RegisterConfigRequest request
                = RegisterConfigRequest.newBuilder()
                    .setConfigDefinition(configDefinition)
                    .setConfigComment(configComment).build();
            
            RegisterConfigResponse response
                = this.getBlockingStub().registerConfig(request);

            return response.getResult();
        });      
    }

    @Override
    public long registerConfig(String configDefinition) 
        throws SzException 
    {
        return this.env.execute(() -> {
            String configComment = createConfigComment(configDefinition);

            RegisterConfigRequest request
                = RegisterConfigRequest.newBuilder()
                    .setConfigDefinition(configDefinition)
                    .setConfigComment(configComment).build();
            
            RegisterConfigResponse response
                = this.getBlockingStub().registerConfig(request);

            return response.getResult();
        });      
    }

    @Override
    public String getConfigRegistry() throws SzException {
        return this.env.execute(() -> {

            GetConfigRegistryRequest request
                = GetConfigRegistryRequest.newBuilder().build();
            
            GetConfigRegistryResponse response
                = this.getBlockingStub().getConfigRegistry(request);

            return response.getResult();
        });      
    }

    @Override
    public long getDefaultConfigId() throws SzException {
        return this.env.execute(() -> {
            GetDefaultConfigIdRequest request
                = GetDefaultConfigIdRequest.newBuilder().build();
            
            GetDefaultConfigIdResponse response
                = this.getBlockingStub().getDefaultConfigId(request);

            return response.getResult();
        });  
    }

    @Override
    public void replaceDefaultConfigId(long currentDefaultConfigId, 
                                       long newDefaultConfigId)
            throws SzReplaceConflictException, SzException 
    {
        this.env.execute(() -> {
            ReplaceDefaultConfigIdRequest request
                = ReplaceDefaultConfigIdRequest.newBuilder()
                    .setCurrentDefaultConfigId(currentDefaultConfigId)
                    .setNewDefaultConfigId(newDefaultConfigId).build();
            
            ReplaceDefaultConfigIdResponse response
                = this.getBlockingStub().replaceDefaultConfigId(request);

            return null;
        });
    }

    @Override
    public void setDefaultConfigId(long configId) throws SzException {
        this.env.execute(() -> {
            SetDefaultConfigIdRequest request
                = SetDefaultConfigIdRequest.newBuilder()
                    .setConfigId(configId).build();
            
            SetDefaultConfigIdResponse response
                = this.getBlockingStub().setDefaultConfigId(request);

            return null;
        });
    }

    @Override
    public long setDefaultConfig(String configDefinition, String configComment) 
        throws SzException 
    {
        return this.env.execute(() -> {
            SetDefaultConfigRequest request
                = SetDefaultConfigRequest.newBuilder()
                    .setConfigDefinition(configDefinition)
                    .setConfigComment(configComment).build();
            
            SetDefaultConfigResponse response
                = this.getBlockingStub().setDefaultConfig(request);

            return response.getResult();
        });      
    }

    @Override
    public long setDefaultConfig(String configDefinition) throws SzException {
        return this.env.execute(() -> {
            String configComment = createConfigComment(configDefinition);

            SetDefaultConfigRequest request
                = SetDefaultConfigRequest.newBuilder()
                    .setConfigDefinition(configDefinition)
                    .setConfigComment(configComment).build();
            
            SetDefaultConfigResponse response
                = this.getBlockingStub().setDefaultConfig(request);

            return response.getResult();
        });      
    }
    
}

package com.senzing.sdk.grpc;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzReplaceConflictException;

public class SzGrpcConfigManager implements SzConfigManager {

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
    public String getConfigs() throws SzException {
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

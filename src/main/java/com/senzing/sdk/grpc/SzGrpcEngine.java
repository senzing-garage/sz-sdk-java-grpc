package com.senzing.sdk.grpc;

import java.util.Set;

import io.grpc.Channel;

import com.senzing.sdk.SzBadInputException;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEntityIds;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzFlag;
import com.senzing.sdk.SzNotFoundException;
import com.senzing.sdk.SzRecordKey;
import com.senzing.sdk.SzRecordKeys;
import com.senzing.sdk.SzUnknownDataSourceException;
import com.senzing.sdk.grpc.SzEngineGrpc.SzEngineBlockingStub;

/**
 * The gRPC implementation of {@link SzEngine}.
 */
public class SzGrpcEngine implements SzEngine {
    /**
     * The {@link SzGrpcEnvironment} that constructed this instance.
     */
    private SzGrpcEnvironment env = null;

    /**
     * The underlying blocking stub.
     */
    private SzEngineBlockingStub blockingStub = null;

    /**
     * Package-access constructor.
     * 
     * @param environment the {@link SzGrpcEnvironment} with which to construct.
     */
    SzGrpcEngine(SzGrpcEnvironment environment) {
        this.env = environment;
        
        Channel channel = this.env.getChannel();

        this.blockingStub = SzEngineGrpc.newBlockingStub(channel);
    }

    /**
     * Gets the underlying {@link SzEngineBlockingStub} for this instance.
     */
    SzEngineBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    @Override
    public void primeEngine() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'primeEngine'");
    }

    @Override
    public String getStats() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStats'");
    }

    @Override
    public String addRecord(SzRecordKey recordKey, String recordDefinition, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzBadInputException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addRecord'");
    }

    @Override
    public String getRecordPreview(String recordDefinition, Set<SzFlag> flags) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'preprocessRecord'");
    }

    @Override
    public String deleteRecord(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteRecord'");
    }

    @Override
    public String reevaluateRecord(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'reevaluateRecord'");
    }

    @Override
    public String reevaluateEntity(long entityId, Set<SzFlag> flags) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'reevaluateEntity'");
    }

    @Override
    public String searchByAttributes(String attributes, String searchProfile, Set<SzFlag> flags) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'searchByAttributes'");
    }

    @Override
    public String searchByAttributes(String attributes, Set<SzFlag> flags) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'searchByAttributes'");
    }

    @Override
    public String whySearch(String attributes, long entityId, String searchProfile, Set<SzFlag> flags)
            throws SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'whySearch'");
    }

    @Override
    public String getEntity(long entityId, Set<SzFlag> flags) throws SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEntity'");
    }

    @Override
    public String getEntity(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEntity'");
    }

    @Override
    public String findInterestingEntities(long entityId, Set<SzFlag> flags) throws SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'findInterestingEntities'");
    }

    @Override
    public String findInterestingEntities(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'findInterestingEntities'");
    }

    @Override
    public String findPath(long startEntityId, long endEntityId, int maxDegrees, SzEntityIds avoidEntityIds,
            Set<String> requiredDataSources, Set<SzFlag> flags)
            throws SzNotFoundException, SzUnknownDataSourceException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'findPath'");
    }

    @Override
    public String findPath(SzRecordKey startRecordKey, SzRecordKey endRecordKey, int maxDegrees,
            SzRecordKeys avoidRecordKeys, Set<String> requiredDataSources, Set<SzFlag> flags)
            throws SzNotFoundException, SzUnknownDataSourceException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'findPath'");
    }

    @Override
    public String findNetwork(SzEntityIds entityIds, int maxDegrees, int buildOutDegrees, int buildOutMaxEntities,
            Set<SzFlag> flags) throws SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'findNetwork'");
    }

    @Override
    public String findNetwork(SzRecordKeys recordKeys, int maxDegrees, int buildOutDegrees, int buildOutMaxEntities,
            Set<SzFlag> flags) throws SzUnknownDataSourceException, SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'findNetwork'");
    }

    @Override
    public String whyRecordInEntity(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'whyRecordInEntity'");
    }

    @Override
    public String whyRecords(SzRecordKey recordKey1, SzRecordKey recordKey2, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'whyRecords'");
    }

    @Override
    public String whyEntities(long entityId1, long entityId2, Set<SzFlag> flags)
            throws SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'whyEntities'");
    }

    @Override
    public String howEntity(long entityId, Set<SzFlag> flags) throws SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'howEntity'");
    }

    @Override
    public String getVirtualEntity(Set<SzRecordKey> recordKeys, Set<SzFlag> flags)
            throws SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getVirtualEntity'");
    }

    @Override
    public String getRecord(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzNotFoundException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRecord'");
    }

    @Override
    public long exportJsonEntityReport(Set<SzFlag> flags) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'exportJsonEntityReport'");
    }

    @Override
    public long exportCsvEntityReport(String csvColumnList, Set<SzFlag> flags) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'exportCsvEntityReport'");
    }

    @Override
    public String fetchNext(long exportHandle) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'fetchNext'");
    }

    @Override
    public void closeExportReport(long exportHandle) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'closeExport'");
    }

    @Override
    public String processRedoRecord(String redoRecord, Set<SzFlag> flags) throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processRedoRecord'");
    }

    @Override
    public String getRedoRecord() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRedoRecord'");
    }

    @Override
    public long countRedoRecords() throws SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'countRedoRecords'");
    }
    
}

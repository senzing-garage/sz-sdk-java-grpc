package com.senzing.sdk.grpc.server;

import io.grpc.stub.StreamObserver;

import java.io.StringReader;

import java.util.Objects;
import java.util.Set;
import java.util.LinkedHashSet;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonString;

import com.senzing.sdk.SzFlag;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzRecordKey;
import com.senzing.sdk.SzRecordKeys;
import com.senzing.sdk.SzEntityIds;
import com.senzing.sdk.SzEnvironment;

import static com.senzing.sdk.grpc.proto.SzEngineGrpc.*;
import static com.senzing.sdk.grpc.proto.SzEngineProto.*;
import static com.senzing.sdk.SzFlagUsageGroup.*;
import static com.senzing.sdk.grpc.server.SzGrpcServer.*;

/**
 * Provides the gRPC server-side implementation for {@link SzEngine}.
 */
public class SzGrpcEngineImpl extends SzEngineImplBase {
    /**
     * The {@link SzGrpcServer} to use.
     */
    private SzGrpcServer server = null;

    /**
     * Constructs with the {@link SzGrpcServer}.
     * 
     * @param server The {@link SzGrpcServer} to use.
     */
    protected SzGrpcEngineImpl(SzGrpcServer server) {
        Objects.requireNonNull(server, "The environment cannot be null");
        if (server.isDestroyed()) {
            throw new IllegalArgumentException(
                "The specified SzCoreEnvironment has already been destroyed");
        }
        this.server = server;
    }

    /**
     * Gets the {@link SzEnvironment} to use from the backing
     * {@link SzGrpcServer}.
     * 
     * @return The {@link SzEnvironment} for the backing server.
     */
    protected SzEnvironment getEnvironment() {
        return this.server.getEnvironment();
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#addRecord(SzRecordKey,String,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void addRecord(AddRecordRequest                  request, 
                          StreamObserver<AddRecordResponse> responseObserver) 
    {
        try {
            String dataSourceCode   = request.getDataSourceCode();
            String recordId         = request.getRecordId();
            String recordDefinition = request.getRecordDefinition();
            long   flags            = request.getFlags();

            SzRecordKey recordKey   = SzRecordKey.of(dataSourceCode, recordId);
            Set<SzFlag> flagSet     = SZ_ADD_RECORD_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.addRecord(recordKey, 
                                             recordDefinition,
                                             flagSet);

            AddRecordResponse.Builder builder 
                = AddRecordResponse.newBuilder();
            if (result != null) {
                builder.setResult(result);
            }
            AddRecordResponse response = builder.build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to defer to the base implementation to throw a 
     * {@link io.grpc.StatusRuntimeException} indicating that the
     * operation is not implemented.
     * <p>
     * <b>NOTE:</b> Remove this method once it is removed from the
     * engine proto file.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void closeExportReport(CloseExportReportRequest request,
            StreamObserver<CloseExportReportResponse> responseObserver) {
        // Leave this unimplemented since gRPC client should stream exports
        super.closeExportReport(request, responseObserver);
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#countRedoRecords()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void countRedoRecords(CountRedoRecordsRequest request,
            StreamObserver<CountRedoRecordsResponse> responseObserver) {
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            long result = engine.countRedoRecords();

            CountRedoRecordsResponse response
                = CountRedoRecordsResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#deleteRecord(SzRecordKey,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void deleteRecord(DeleteRecordRequest request, StreamObserver<DeleteRecordResponse> responseObserver) {
        try {
            String dataSourceCode   = request.getDataSourceCode();
            String recordId         = request.getRecordId();
            long   flags            = request.getFlags();

            SzRecordKey recordKey   = SzRecordKey.of(dataSourceCode, recordId);
            Set<SzFlag> flagSet     = SZ_DELETE_RECORD_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.deleteRecord(recordKey, flagSet);

            DeleteRecordResponse.Builder builder
                = DeleteRecordResponse.newBuilder();
            if (result != null) {
                builder.setResult(result);
            }
            DeleteRecordResponse response = builder.build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
   }

    /**
     * Implemented to defer to the base implementation to throw a 
     * {@link io.grpc.StatusRuntimeException} indicating that the
     * operation is not implemented.
     * <p>
     * <b>NOTE:</b> Remove this method once it is removed from the
     * engine proto file.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void exportCsvEntityReport(ExportCsvEntityReportRequest request,
            StreamObserver<ExportCsvEntityReportResponse> responseObserver) {
        // Leave this unimplemented since gRPC client should stream exports
        super.exportCsvEntityReport(request, responseObserver);
    }

    /**
     * Implemented to defer to the base implementation to throw a 
     * {@link io.grpc.StatusRuntimeException} indicating that the
     * operation is not implemented.
     * <p>
     * <b>NOTE:</b> Remove this method once it is removed from the
     * engine proto file.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void exportJsonEntityReport(ExportJsonEntityReportRequest request,
            StreamObserver<ExportJsonEntityReportResponse> responseObserver) {
        // Leave this unimplemented since gRPC client should stream exports
        super.exportJsonEntityReport(request, responseObserver);
    }

    /**
     * Implemented to defer to the base implementation to throw a 
     * {@link io.grpc.StatusRuntimeException} indicating that the
     * operation is not implemented.
     * <p>
     * <b>NOTE:</b> Remove this method once it is removed from the
     * engine proto file.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void fetchNext(FetchNextRequest request, StreamObserver<FetchNextResponse> responseObserver) {
        // Leave this unimplemented since gRPC client should stream exports
        super.fetchNext(request, responseObserver);
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#findInterestingEntities(long,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void findInterestingEntitiesByEntityId(FindInterestingEntitiesByEntityIdRequest request,
            StreamObserver<FindInterestingEntitiesByEntityIdResponse> responseObserver) {
        try {
            Long        entityId    = request.getEntityId();
            long        flags       = request.getFlags();
            Set<SzFlag> flagSet     = SZ_FIND_INTERESTING_ENTITIES_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.findInterestingEntities(entityId, flagSet);

            FindInterestingEntitiesByEntityIdResponse response
                = FindInterestingEntitiesByEntityIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#findInterestingEntities(SzRecordKey,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void findInterestingEntitiesByRecordId(FindInterestingEntitiesByRecordIdRequest request,
            StreamObserver<FindInterestingEntitiesByRecordIdResponse> responseObserver) {
        try {
            String dataSourceCode   = request.getDataSourceCode();
            String recordId         = request.getRecordId();
            long   flags            = request.getFlags();

            SzRecordKey recordKey   = SzRecordKey.of(dataSourceCode, recordId);
            Set<SzFlag> flagSet     = SZ_FIND_INTERESTING_ENTITIES_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.findInterestingEntities(recordKey, flagSet);

            FindInterestingEntitiesByRecordIdResponse response
                = FindInterestingEntitiesByRecordIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Internal method for parsing the entity ID's JSON.
     * 
     * @param entityIdsJson The JSON text to parse.
     * 
     * @return The {@link Set} of {@link Long} entity ID's.
     */
    private static Set<Long> parseEntityIds(String entityIdsJson) {
        if (entityIdsJson == null || entityIdsJson.trim().length() == 0) 
        {
            return null;
        }
        StringReader    sr = new StringReader(entityIdsJson);
        JsonObject      jsonObj = Json.createReader(sr).readObject();
        JsonArray       jsonArr = jsonObj.getJsonArray("ENTITIES");

        Set<Long> entityIds = new LinkedHashSet<>();
        for (JsonObject elem : jsonArr.getValuesAs(JsonObject.class)) {
            entityIds.add(elem.getJsonNumber("ENTITY_ID").longValue());
        }
        return entityIds;
    }

    /**
     * Internal method for parsing the record keys JSON.
     * 
     * @param recordKeysJson The JSON text to parse.
     * 
     * @return The {@link Set} of {@link SzRecordKey} instances.
     */
    private static Set<SzRecordKey> parseRecordKeys(String recordKeysJson) {
        if (recordKeysJson == null || recordKeysJson.trim().length() == 0)
        {
            return null;
        }
        StringReader    sr = new StringReader(recordKeysJson);
        JsonObject      jsonObj = Json.createReader(sr).readObject();
        JsonArray       jsonArr = jsonObj.getJsonArray("RECORDS");

        Set<SzRecordKey> recordKeys = new LinkedHashSet<>();
        for (JsonObject elem : jsonArr.getValuesAs(JsonObject.class)) {
            String dataSource = elem.getString("DATA_SOURCE");
            String recordId   = elem.getString("RECORD_ID");

            SzRecordKey recordKey = SzRecordKey.of(dataSource, recordId);
            recordKeys.add(recordKey);
        }
        return recordKeys;
    }

    /**
     * Internal method for parsing the data sources JSON.
     * 
     * @param dataSourcesJson The JSON text to parse.
     * 
     * @return The {@link Set} of {@link String} data source codes.
     */
    private static Set<String> parseDataSources(String dataSourcesJson) {
        if (dataSourcesJson == null || dataSourcesJson.trim().length() == 0)
        {
            return null;
        }
        StringReader    sr = new StringReader(dataSourcesJson);
        JsonObject      jsonObj = Json.createReader(sr).readObject();
        JsonArray       jsonArr = jsonObj.getJsonArray("DATA_SOURCES");

        Set<String> dataSources = new LinkedHashSet<>();
        for (JsonString elem : jsonArr.getValuesAs(JsonString.class)) {
            dataSources.add(elem.getString());
        }
        return dataSources;
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#findNetwork(SzEntityIds,int,int,int,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void findNetworkByEntityId(FindNetworkByEntityIdRequest request,
            StreamObserver<FindNetworkByEntityIdResponse> responseObserver) 
    {
        try {
            String  entityIdsJson       = request.getEntityIds();
            int     maxDegrees          = (int) request.getMaxDegrees();
            int     buildOutDegrees     = (int) request.getBuildOutDegrees();
            int     buildOutMaxEntities = (int) request.getBuildOutMaxEntities();
            long    flags               = request.getFlags();

            Set<Long> entityIds = parseEntityIds(entityIdsJson);

            Set<SzFlag> flagSet = SZ_FIND_NETWORK_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.findNetwork(
                SzEntityIds.of(entityIds),
                maxDegrees,
                buildOutDegrees,
                buildOutMaxEntities,
                flagSet);
                
            FindNetworkByEntityIdResponse response
                = FindNetworkByEntityIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#findNetwork(SzRecordKeys,int,int,int,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void findNetworkByRecordId(FindNetworkByRecordIdRequest request,
            StreamObserver<FindNetworkByRecordIdResponse> responseObserver)
    {
        try {
            String  recordKeysJson      = request.getRecordKeys();
            int     maxDegrees          = (int) request.getMaxDegrees();
            int     buildOutDegrees     = (int) request.getBuildOutDegrees();
            int     buildOutMaxEntities = (int) request.getBuildOutMaxEntities();
            long    flags               = request.getFlags();

            Set<SzRecordKey> recordKeys = parseRecordKeys(recordKeysJson);

            Set<SzFlag> flagSet = SZ_FIND_NETWORK_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.findNetwork(
                SzRecordKeys.of(recordKeys),
                maxDegrees,
                buildOutDegrees,
                buildOutMaxEntities,
                flagSet);
                
            FindNetworkByRecordIdResponse response
                = FindNetworkByRecordIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#findPath(long,long,int,SzEntityIds,Set,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void findPathByEntityId(FindPathByEntityIdRequest request,
            StreamObserver<FindPathByEntityIdResponse> responseObserver)
    {
        try {
            long    startEntityId       = request.getStartEntityId();
            long    endEntityId         = request.getEndEntityId();
            int     maxDegrees          = (int) request.getMaxDegrees();
            String  avoidanceJson       = request.getAvoidEntityIds();
            String  dataSourcesJson     = request.getRequiredDataSources();
            long    flags               = request.getFlags();

            Set<Long>   avoidEntityIds  = parseEntityIds(avoidanceJson);
            Set<String> requiredSources = parseDataSources(dataSourcesJson);

            Set<SzFlag> flagSet = SZ_FIND_PATH_FLAGS.toFlagSet(flags);
            
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.findPath(
                startEntityId,
                endEntityId,
                maxDegrees,
                SzEntityIds.of(avoidEntityIds),
                requiredSources,
                flagSet);
                
            FindPathByEntityIdResponse response
                = FindPathByEntityIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#findPath(SzRecordKey,SzRecordKey,int,SzRecordKeys,Set,Set)}
     * method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void findPathByRecordId(FindPathByRecordIdRequest request,
            StreamObserver<FindPathByRecordIdResponse> responseObserver)
    {
        try {
            String  startDataSource     = request.getStartDataSourceCode();
            String  startRecordId       = request.getStartRecordId();
            String  endDataSource       = request.getEndDataSourceCode();
            String  endRecordId         = request.getEndRecordId();
            int     maxDegrees          = (int) request.getMaxDegrees();
            String  avoidanceJson       = request.getAvoidRecordKeys();
            String  dataSourcesJson     = request.getRequiredDataSources();
            long    flags               = request.getFlags();

            Set<SzRecordKey>    avoidRecordKeys = parseRecordKeys(avoidanceJson);
            Set<String>         requiredSources = parseDataSources(dataSourcesJson);

            SzRecordKey startRecordKey  = SzRecordKey.of(startDataSource, startRecordId);
            SzRecordKey endRecordKey    = SzRecordKey.of(endDataSource, endRecordId);
            Set<SzFlag> flagSet         = SZ_FIND_PATH_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.findPath(
                startRecordKey,
                endRecordKey,
                maxDegrees,
                SzRecordKeys.of(avoidRecordKeys),
                requiredSources,
                flagSet);
                
            FindPathByRecordIdResponse response
                = FindPathByRecordIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEnvironment#getActiveConfigId()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void getActiveConfigId(GetActiveConfigIdRequest request,
            StreamObserver<GetActiveConfigIdResponse> responseObserver) 
    {
        try {
            long configId = this.getEnvironment().getActiveConfigId();
                
            GetActiveConfigIdResponse response
                = GetActiveConfigIdResponse
                    .newBuilder().setResult(configId).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#getEntity(long,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void getEntityByEntityId(GetEntityByEntityIdRequest request,
            StreamObserver<GetEntityByEntityIdResponse> responseObserver)
    {
        try {
            long        entityId    = request.getEntityId();
            long        flags       = request.getFlags();
            Set<SzFlag> flagSet     = SZ_ENTITY_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.getEntity(entityId, flagSet);
                
            GetEntityByEntityIdResponse response
                = GetEntityByEntityIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#getEntity(SzRecordKey,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void getEntityByRecordId(GetEntityByRecordIdRequest request,
            StreamObserver<GetEntityByRecordIdResponse> responseObserver) 
    {
        try {
            String      dataSource  = request.getDataSourceCode();
            String      recordId    = request.getRecordId();
            long        flags       = request.getFlags();
            SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);
            Set<SzFlag> flagSet     = SZ_ENTITY_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.getEntity(recordKey, flagSet);
                
            GetEntityByRecordIdResponse response
                = GetEntityByRecordIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#getRecord(SzRecordKey,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void getRecord(GetRecordRequest request, 
        StreamObserver<GetRecordResponse> responseObserver) 
    {
        try {
            String      dataSource  = request.getDataSourceCode();
            String      recordId    = request.getRecordId();
            long        flags       = request.getFlags();
            SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);
            Set<SzFlag> flagSet     = SZ_RECORD_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.getRecord(recordKey, flagSet);
                
            GetRecordResponse response
                = GetRecordResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#getRecordPreview(String,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void getRecordPreview(GetRecordPreviewRequest request,
            StreamObserver<GetRecordPreviewResponse> responseObserver)
    {
        try {
            String      recordDef   = request.getRecordDefinition();
            long        flags       = request.getFlags();
            Set<SzFlag> flagSet     = SZ_RECORD_PREVIEW_FLAGS.toFlagSet(flags);
            
            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.getRecordPreview(recordDef, flagSet);
                
            GetRecordPreviewResponse response
                = GetRecordPreviewResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#getRedoRecord()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void getRedoRecord(GetRedoRecordRequest request, 
        StreamObserver<GetRedoRecordResponse> responseObserver) 
    {
        try {
            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.getRedoRecord();
            
            GetRedoRecordResponse.Builder builder
                = GetRedoRecordResponse.newBuilder();
            if (result != null) {
                builder.setResult(result);
            }
            GetRedoRecordResponse response = builder.build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#getStats()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void getStats(GetStatsRequest request,
        StreamObserver<GetStatsResponse> responseObserver) 
    {
        try {
            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.getStats();
                
            GetStatsResponse response
                = GetStatsResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#getVirtualEntity(Set,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void getVirtualEntityByRecordId(GetVirtualEntityByRecordIdRequest request,
            StreamObserver<GetVirtualEntityByRecordIdResponse> responseObserver) 
    {
        try {
            String  recordKeysJson      = request.getRecordKeys();
            long    flags               = request.getFlags();

            Set<SzRecordKey> recordKeys = parseRecordKeys(recordKeysJson);

            Set<SzFlag> flagSet = SZ_VIRTUAL_ENTITY_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.getVirtualEntity(
                SzRecordKeys.of(recordKeys), flagSet);
                
            GetVirtualEntityByRecordIdResponse response
                = GetVirtualEntityByRecordIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#howEntity(long,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void howEntityByEntityId(HowEntityByEntityIdRequest request,
            StreamObserver<HowEntityByEntityIdResponse> responseObserver) 
    {
        try {
            long        entityId    = request.getEntityId();
            long        flags       = request.getFlags();
            Set<SzFlag> flagSet     = SZ_HOW_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.howEntity(entityId, flagSet);
                
            HowEntityByEntityIdResponse response
                = HowEntityByEntityIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#primeEngine()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void primeEngine(PrimeEngineRequest request,
        StreamObserver<PrimeEngineResponse> responseObserver)
    {
        try {
            SzEngine engine = this.getEnvironment().getEngine();
            
            engine.primeEngine();
                
            PrimeEngineResponse response = PrimeEngineResponse.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#processRedoRecord(String,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void processRedoRecord(ProcessRedoRecordRequest request,
            StreamObserver<ProcessRedoRecordResponse> responseObserver) 
    {
        try {
            String      redoRecord  = request.getRedoRecord();
            long        flags       = request.getFlags();
            Set<SzFlag> flagSet     = SZ_REDO_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.processRedoRecord(redoRecord, flagSet);
            
            ProcessRedoRecordResponse.Builder builder
                = ProcessRedoRecordResponse.newBuilder();
            if (result != null) {
                builder.setResult(result);
            }
            ProcessRedoRecordResponse response = builder.build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#reevaluateEntity(long,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void reevaluateEntity(ReevaluateEntityRequest request,
            StreamObserver<ReevaluateEntityResponse> responseObserver) 
    {
        try {
            long        entityId    = request.getEntityId();
            long        flags       = request.getFlags();
            Set<SzFlag> flagSet     = SZ_REEVALUATE_ENTITY_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.reevaluateEntity(entityId, flagSet);
                
            ReevaluateEntityResponse.Builder builder
                = ReevaluateEntityResponse.newBuilder();
            if (result != null) {
                builder.setResult(result);
            }
            ReevaluateEntityResponse response = builder.build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#reevaluateRecord(SzRecordKey,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void reevaluateRecord(ReevaluateRecordRequest request,
            StreamObserver<ReevaluateRecordResponse> responseObserver)
    {
        try {
            String      dataSource  = request.getDataSourceCode();
            String      recordId    = request.getRecordId();
            long        flags       = request.getFlags();
            SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);
            Set<SzFlag> flagSet     = SZ_REEVALUATE_RECORD_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.reevaluateRecord(recordKey, flagSet);
            
            ReevaluateRecordResponse.Builder builder
                = ReevaluateRecordResponse.newBuilder();
            if (result != null) {
                builder.setResult(result);
            }
            ReevaluateRecordResponse response = builder.build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEnvironment#reinitialize(long)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void reinitialize(ReinitializeRequest request,
        StreamObserver<ReinitializeResponse> responseObserver) 
    {
        try {
            long configId = request.getConfigId();

            this.getEnvironment().reinitialize(configId);
                
            ReinitializeResponse response
                = ReinitializeResponse.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#searchByAttributes(String,String,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void searchByAttributes(SearchByAttributesRequest request,
            StreamObserver<SearchByAttributesResponse> responseObserver) 
    {
        try {
            String      attributes  = request.getAttributes();
            String      profile     = request.getSearchProfile();
            long        flags       = request.getFlags();
            Set<SzFlag> flagSet     = SZ_REEVALUATE_RECORD_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.searchByAttributes(attributes, profile, flagSet);
                
            SearchByAttributesResponse response
                = SearchByAttributesResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#exportCsvEntityReport(String,Set)},
     * {@link SzEngine#fetchNext(long)} and {@link SzEngine#closeExportReport(long)}
     * methods.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void streamExportCsvEntityReport(StreamExportCsvEntityReportRequest request,
            StreamObserver<StreamExportCsvEntityReportResponse> responseObserver)
    {
        try {
            String      csvColumnList   = request.getCsvColumnList();
            long        flags           = request.getFlags();
            Set<SzFlag> flagSet         = SZ_EXPORT_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            long exportHandle = engine.exportCsvEntityReport(csvColumnList, flagSet);

            try {
                for (String line = engine.fetchNext(exportHandle); 
                     line != null;
                     line = engine.fetchNext(exportHandle)) 
                     
                {
                    StreamExportCsvEntityReportResponse response
                        = StreamExportCsvEntityReportResponse
                            .newBuilder().setResult(line).build();
                    
                    responseObserver.onNext(response);
                }

                responseObserver.onCompleted();
 
            } finally {
                engine.closeExportReport(exportHandle);
            }

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#exportJsonEntityReport(Set)},
     * {@link SzEngine#fetchNext(long)} and {@link SzEngine#closeExportReport(long)}
     * methods.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void streamExportJsonEntityReport(StreamExportJsonEntityReportRequest request,
            StreamObserver<StreamExportJsonEntityReportResponse> responseObserver)
    {
        try {
            long        flags   = request.getFlags();
            Set<SzFlag> flagSet = SZ_EXPORT_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            long exportHandle = engine.exportJsonEntityReport(flagSet);

            try {
                for (String line = engine.fetchNext(exportHandle); 
                     line != null;
                     line = engine.fetchNext(exportHandle)) 
                     
                {
                    StreamExportJsonEntityReportResponse response
                        = StreamExportJsonEntityReportResponse
                            .newBuilder().setResult(line).build();

                    responseObserver.onNext(response);
                }

                responseObserver.onCompleted();
 
            } finally {
                engine.closeExportReport(exportHandle);
            }

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#whyEntities(long,long,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void whyEntities(WhyEntitiesRequest request,
        StreamObserver<WhyEntitiesResponse> responseObserver) 
    {
        try {
            long        entityId1   = request.getEntityId1();
            long        entityId2   = request.getEntityId2();
            long        flags       = request.getFlags();
            Set<SzFlag> flagSet     = SZ_WHY_ENTITIES_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.whyEntities(entityId1, entityId2, flagSet);
                
            WhyEntitiesResponse response
                = WhyEntitiesResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#whyRecordInEntity(SzRecordKey,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void whyRecordInEntity(WhyRecordInEntityRequest request,
            StreamObserver<WhyRecordInEntityResponse> responseObserver) 
    {
        try {
            String      dataSource  = request.getDataSourceCode();
            String      recordId    = request.getRecordId();
            long        flags       = request.getFlags();
            SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);
            Set<SzFlag> flagSet     = SZ_WHY_RECORD_IN_ENTITY_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.whyRecordInEntity(recordKey, flagSet);
                
            WhyRecordInEntityResponse response
                = WhyRecordInEntityResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#whyRecords(SzRecordKey,SzRecordKey,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void whyRecords(WhyRecordsRequest request, 
        StreamObserver<WhyRecordsResponse> responseObserver) 
    {
        try {
            String      dataSource1 = request.getDataSourceCode1();
            String      recordId1   = request.getRecordId1();
            String      dataSource2 = request.getDataSourceCode2();
            String      recordId2   = request.getRecordId2();
            long        flags       = request.getFlags();
            SzRecordKey recordKey1  = SzRecordKey.of(dataSource1, recordId1);
            SzRecordKey recordKey2  = SzRecordKey.of(dataSource2, recordId2);
            Set<SzFlag> flagSet     = SZ_WHY_RECORDS_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.whyRecords(recordKey1, recordKey2, flagSet);
                
            WhyRecordsResponse response
                = WhyRecordsResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzEngine#whySearch(String,long,String,Set)} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
    @Override
    public void whySearch(WhySearchRequest request,
        StreamObserver<WhySearchResponse> responseObserver) 
    {
        try {
            String      attributes  = request.getAttributes();
            long        entityId    = request.getEntityId();
            String      profile     = request.getSearchProfile();
            long        flags       = request.getFlags();
            Set<SzFlag> flagSet     = SZ_WHY_SEARCH_FLAGS.toFlagSet(flags);

            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.whySearch(attributes, entityId, profile, flagSet);
                
            WhySearchResponse response
                = WhySearchResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

}

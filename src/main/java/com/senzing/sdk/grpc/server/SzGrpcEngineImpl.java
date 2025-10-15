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
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzRecordKey;
import com.senzing.sdk.SzRecordKeys;
import com.senzing.sdk.SzEntityIds;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.grpc.server.SzEngineGrpc.SzEngineImplBase;
import com.senzing.sdk.grpc.server.SzEngineProto.AddRecordRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.AddRecordResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.CloseExportReportRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.CloseExportReportResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.CountRedoRecordsRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.CountRedoRecordsResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.DeleteRecordRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.DeleteRecordResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.ExportCsvEntityReportRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.ExportCsvEntityReportResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.ExportJsonEntityReportRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.ExportJsonEntityReportResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.FetchNextRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.FetchNextResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.FindInterestingEntitiesByEntityIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.FindInterestingEntitiesByEntityIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.FindInterestingEntitiesByRecordIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.FindInterestingEntitiesByRecordIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.FindNetworkByEntityIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.FindNetworkByEntityIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.FindNetworkByRecordIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.FindNetworkByRecordIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.FindPathByEntityIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.FindPathByEntityIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.FindPathByRecordIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.FindPathByRecordIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.GetActiveConfigIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.GetActiveConfigIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.GetEntityByEntityIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.GetEntityByEntityIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.GetEntityByRecordIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.GetEntityByRecordIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.GetRecordPreviewRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.GetRecordPreviewResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.GetRecordRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.GetRecordResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.GetRedoRecordRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.GetRedoRecordResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.GetStatsRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.GetStatsResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.GetVirtualEntityByRecordIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.GetVirtualEntityByRecordIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.HowEntityByEntityIdRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.HowEntityByEntityIdResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.PrimeEngineRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.PrimeEngineResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.ProcessRedoRecordRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.ProcessRedoRecordResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.ReevaluateEntityRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.ReevaluateEntityResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.ReevaluateRecordRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.ReevaluateRecordResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.ReinitializeRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.ReinitializeResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.SearchByAttributesRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.SearchByAttributesResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.StreamExportCsvEntityReportRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.StreamExportCsvEntityReportResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.StreamExportJsonEntityReportRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.StreamExportJsonEntityReportResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.WhyEntitiesRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.WhyEntitiesResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.WhyRecordInEntityRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.WhyRecordInEntityResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.WhyRecordsRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.WhyRecordsResponse;
import com.senzing.sdk.grpc.server.SzEngineProto.WhySearchRequest;
import com.senzing.sdk.grpc.server.SzEngineProto.WhySearchResponse;

import static com.senzing.sdk.SzFlagUsageGroup.*;

/**
 * Provides the gRPC server-side implementation for {@link SzEngine}.
 */
class SzGrpcEngineImpl extends SzEngineImplBase {
    /**
     * The {@link SzGrpcServer} to use.
     */
    SzGrpcServer server = null;

    /**
     * Constructs with the {@link SzGrpcServer}.
     * 
     * @param server The {@link SzGrpcServer} to use.
     */
    public SzGrpcEngineImpl(SzGrpcServer server) {
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
    private SzEnvironment getEnvironment() {
        return this.server.getEnvironment();
    }

    @Override
    public void addRecord(AddRecordRequest                  request, 
                          StreamObserver<AddRecordResponse> responseObserver) 
    {
        String dataSourceCode   = request.getDataSourceCode();
        String recordId         = request.getRecordId();
        String recordDefinition = request.getRecordDefinition();
        long   flags            = request.getFlags();

        SzRecordKey recordKey   = SzRecordKey.of(dataSourceCode, recordId);
        Set<SzFlag> flagSet     = SZ_ADD_RECORD_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.addRecord(recordKey, 
                                             recordDefinition,
                                             flagSet);

            AddRecordResponse response
                = AddRecordResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void closeExportReport(CloseExportReportRequest request,
            StreamObserver<CloseExportReportResponse> responseObserver) {
        // Leave this unimplemented since gRPC client should stream exports
        super.closeExportReport(request, responseObserver);
    }

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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void deleteRecord(DeleteRecordRequest request, StreamObserver<DeleteRecordResponse> responseObserver) {
        String dataSourceCode   = request.getDataSourceCode();
        String recordId         = request.getRecordId();
        long   flags            = request.getFlags();

        SzRecordKey recordKey   = SzRecordKey.of(dataSourceCode, recordId);
        Set<SzFlag> flagSet     = SZ_DELETE_RECORD_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.deleteRecord(recordKey, flagSet);

            DeleteRecordResponse response
                = DeleteRecordResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
   }

    @Override
    public void exportCsvEntityReport(ExportCsvEntityReportRequest request,
            StreamObserver<ExportCsvEntityReportResponse> responseObserver) {
        // Leave this unimplemented since gRPC client should stream exports
        super.exportCsvEntityReport(request, responseObserver);
    }

    @Override
    public void exportJsonEntityReport(ExportJsonEntityReportRequest request,
            StreamObserver<ExportJsonEntityReportResponse> responseObserver) {
        // Leave this unimplemented since gRPC client should stream exports
        super.exportJsonEntityReport(request, responseObserver);
    }

    @Override
    public void fetchNext(FetchNextRequest request, StreamObserver<FetchNextResponse> responseObserver) {
        // Leave this unimplemented since gRPC client should stream exports
        super.fetchNext(request, responseObserver);
    }

    @Override
    public void findInterestingEntitiesByEntityId(FindInterestingEntitiesByEntityIdRequest request,
            StreamObserver<FindInterestingEntitiesByEntityIdResponse> responseObserver) {
        Long        entityId    = request.getEntityId();
        long        flags       = request.getFlags();
        Set<SzFlag> flagSet     = SZ_FIND_INTERESTING_ENTITIES_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.findInterestingEntities(entityId, flagSet);

            FindInterestingEntitiesByEntityIdResponse response
                = FindInterestingEntitiesByEntityIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void findInterestingEntitiesByRecordId(FindInterestingEntitiesByRecordIdRequest request,
            StreamObserver<FindInterestingEntitiesByRecordIdResponse> responseObserver) {
        String dataSourceCode   = request.getDataSourceCode();
        String recordId         = request.getRecordId();
        long   flags            = request.getFlags();

        SzRecordKey recordKey   = SzRecordKey.of(dataSourceCode, recordId);
        Set<SzFlag> flagSet     = SZ_FIND_INTERESTING_ENTITIES_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.findInterestingEntities(recordKey, flagSet);

            FindInterestingEntitiesByRecordIdResponse response
                = FindInterestingEntitiesByRecordIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    private static Set<Long> parseEntityIds(String entityIdsJson) {
        if (entityIdsJson == null) {
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

    private static Set<SzRecordKey> parseRecordKeys(String recordKeysJson) {
        if (recordKeysJson == null) {
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

    private static Set<String> parseDataSources(String dataSourcesJson) {
        if (dataSourcesJson == null) {
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

    @Override
    public void findNetworkByEntityId(FindNetworkByEntityIdRequest request,
            StreamObserver<FindNetworkByEntityIdResponse> responseObserver) 
    {
        String  entityIdsJson       = request.getEntityIds();
        int     maxDegrees          = (int) request.getMaxDegrees();
        int     buildOutDegrees     = (int) request.getBuildOutDegrees();
        int     buildOutMaxEntities = (int) request.getBuildOutMaxEntities();
        long    flags               = request.getFlags();

        Set<Long> entityIds = parseEntityIds(entityIdsJson);

        Set<SzFlag> flagSet = SZ_FIND_NETWORK_FLAGS.toFlagSet(flags);
        try {
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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void findNetworkByRecordId(FindNetworkByRecordIdRequest request,
            StreamObserver<FindNetworkByRecordIdResponse> responseObserver)
    {
        String  recordKeysJson      = request.getRecordKeys();
        int     maxDegrees          = (int) request.getMaxDegrees();
        int     buildOutDegrees     = (int) request.getBuildOutDegrees();
        int     buildOutMaxEntities = (int) request.getBuildOutMaxEntities();
        long    flags               = request.getFlags();

        Set<SzRecordKey> recordKeys = parseRecordKeys(recordKeysJson);

        Set<SzFlag> flagSet = SZ_FIND_NETWORK_FLAGS.toFlagSet(flags);
        try {
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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void findPathByEntityId(FindPathByEntityIdRequest request,
            StreamObserver<FindPathByEntityIdResponse> responseObserver)
    {
        long    startEntityId       = request.getStartEntityId();
        long    endEntityId         = request.getEndEntityId();
        int     maxDegrees          = (int) request.getMaxDegrees();
        String  avoidanceJson       = request.getAvoidEntityIds();
        String  dataSourcesJson     = request.getRequiredDataSources();
        long    flags               = request.getFlags();

        Set<Long>   avoidEntityIds  = parseEntityIds(avoidanceJson);
        Set<String> requiredSources = parseDataSources(dataSourcesJson);

        Set<SzFlag> flagSet = SZ_FIND_PATH_FLAGS.toFlagSet(flags);
        try {
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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void findPathByRecordId(FindPathByRecordIdRequest request,
            StreamObserver<FindPathByRecordIdResponse> responseObserver)
    {
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
        try {
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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getEntityByEntityId(GetEntityByEntityIdRequest request,
            StreamObserver<GetEntityByEntityIdResponse> responseObserver)
    {
        long        entityId    = request.getEntityId();
        long        flags       = request.getFlags();
        Set<SzFlag> flagSet     = SZ_ENTITY_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.getEntity(entityId, flagSet);
                
            GetEntityByEntityIdResponse response
                = GetEntityByEntityIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getEntityByRecordId(GetEntityByRecordIdRequest request,
            StreamObserver<GetEntityByRecordIdResponse> responseObserver) 
    {
        String      dataSource  = request.getDataSourceCode();
        String      recordId    = request.getRecordId();
        long        flags       = request.getFlags();
        SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);
        Set<SzFlag> flagSet     = SZ_ENTITY_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.getEntity(recordKey, flagSet);
                
            GetEntityByRecordIdResponse response
                = GetEntityByRecordIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getRecord(GetRecordRequest request, 
        StreamObserver<GetRecordResponse> responseObserver) 
    {
        String      dataSource  = request.getDataSourceCode();
        String      recordId    = request.getRecordId();
        long        flags       = request.getFlags();
        SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);
        Set<SzFlag> flagSet     = SZ_RECORD_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.getRecord(recordKey, flagSet);
                
            GetRecordResponse response
                = GetRecordResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getRecordPreview(GetRecordPreviewRequest request,
            StreamObserver<GetRecordPreviewResponse> responseObserver)
    {
        String      recordDef   = request.getRecordDefinition();
        long        flags       = request.getFlags();
        Set<SzFlag> flagSet     = SZ_RECORD_PREVIEW_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.getRecordPreview(recordDef, flagSet);
                
            GetRecordPreviewResponse response
                = GetRecordPreviewResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getRedoRecord(GetRedoRecordRequest request, 
        StreamObserver<GetRedoRecordResponse> responseObserver) 
    {
        try {
            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.getRedoRecord();
                
            GetRedoRecordResponse response
                = GetRedoRecordResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getVirtualEntityByRecordId(GetVirtualEntityByRecordIdRequest request,
            StreamObserver<GetVirtualEntityByRecordIdResponse> responseObserver) 
    {
        String  recordKeysJson      = request.getRecordKeys();
        long    flags               = request.getFlags();

        Set<SzRecordKey> recordKeys = parseRecordKeys(recordKeysJson);

        Set<SzFlag> flagSet = SZ_VIRTUAL_ENTITY_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.getVirtualEntity(
                SzRecordKeys.of(recordKeys), flagSet);
                
            GetVirtualEntityByRecordIdResponse response
                = GetVirtualEntityByRecordIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void howEntityByEntityId(HowEntityByEntityIdRequest request,
            StreamObserver<HowEntityByEntityIdResponse> responseObserver) 
    {
        long        entityId    = request.getEntityId();
        long        flags       = request.getFlags();
        Set<SzFlag> flagSet     = SZ_HOW_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.howEntity(entityId, flagSet);
                
            HowEntityByEntityIdResponse response
                = HowEntityByEntityIdResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void processRedoRecord(ProcessRedoRecordRequest request,
            StreamObserver<ProcessRedoRecordResponse> responseObserver) 
    {
        String      redoRecord  = request.getRedoRecord();
        long        flags       = request.getFlags();
        Set<SzFlag> flagSet     = SZ_REDO_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();
            
            String result = engine.processRedoRecord(redoRecord, flagSet);
                
            ProcessRedoRecordResponse response
                = ProcessRedoRecordResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void reevaluateEntity(ReevaluateEntityRequest request,
            StreamObserver<ReevaluateEntityResponse> responseObserver) 
    {
        long        entityId    = request.getEntityId();
        long        flags       = request.getFlags();
        Set<SzFlag> flagSet     = SZ_REEVALUATE_ENTITY_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.reevaluateEntity(entityId, flagSet);
                
            ReevaluateEntityResponse response
                = ReevaluateEntityResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void reevaluateRecord(ReevaluateRecordRequest request,
            StreamObserver<ReevaluateRecordResponse> responseObserver)
    {
        String      dataSource  = request.getDataSourceCode();
        String      recordId    = request.getRecordId();
        long        flags       = request.getFlags();
        SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);
        Set<SzFlag> flagSet     = SZ_REEVALUATE_RECORD_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.reevaluateRecord(recordKey, flagSet);
                
            ReevaluateRecordResponse response
                = ReevaluateRecordResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void reinitialize(ReinitializeRequest request,
        StreamObserver<ReinitializeResponse> responseObserver) 
    {
        long configId = request.getConfigId();
        try {
            this.getEnvironment().reinitialize(configId);
                
            ReinitializeResponse response
                = ReinitializeResponse.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void searchByAttributes(SearchByAttributesRequest request,
            StreamObserver<SearchByAttributesResponse> responseObserver) 
    {
        String      attributes  = request.getAttributes();
        String      profile     = request.getSearchProfile();
        long        flags       = request.getFlags();
        Set<SzFlag> flagSet     = SZ_REEVALUATE_RECORD_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.searchByAttributes(attributes, profile, flagSet);
                
            SearchByAttributesResponse response
                = SearchByAttributesResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void streamExportCsvEntityReport(StreamExportCsvEntityReportRequest request,
            StreamObserver<StreamExportCsvEntityReportResponse> responseObserver)
    {
        String      csvColumnList   = request.getCsvColumnList();
        long        flags           = request.getFlags();
        Set<SzFlag> flagSet         = SZ_EXPORT_FLAGS.toFlagSet(flags);
        try {
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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void streamExportJsonEntityReport(StreamExportJsonEntityReportRequest request,
            StreamObserver<StreamExportJsonEntityReportResponse> responseObserver)
    {
        long        flags   = request.getFlags();
        Set<SzFlag> flagSet = SZ_EXPORT_FLAGS.toFlagSet(flags);
        try {
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

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void whyEntities(WhyEntitiesRequest request,
        StreamObserver<WhyEntitiesResponse> responseObserver) 
    {
        long        entityId1   = request.getEntityId1();
        long        entityId2   = request.getEntityId2();
        long        flags       = request.getFlags();
        Set<SzFlag> flagSet     = SZ_WHY_ENTITIES_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.whyEntities(entityId1, entityId2, flagSet);
                
            WhyEntitiesResponse response
                = WhyEntitiesResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void whyRecordInEntity(WhyRecordInEntityRequest request,
            StreamObserver<WhyRecordInEntityResponse> responseObserver) 
    {
        String      dataSource  = request.getDataSourceCode();
        String      recordId    = request.getRecordId();
        long        flags       = request.getFlags();
        SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);
        Set<SzFlag> flagSet     = SZ_WHY_RECORD_IN_ENTITY_FLAGS.toFlagSet(flags);
        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.whyRecordInEntity(recordKey, flagSet);
                
            WhyRecordInEntityResponse response
                = WhyRecordInEntityResponse
                    .newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void whyRecords(WhyRecordsRequest request, 
        StreamObserver<WhyRecordsResponse> responseObserver) 
    {
        String      dataSource1 = request.getDataSourceCode1();
        String      recordId1   = request.getRecordId1();
        String      dataSource2 = request.getDataSourceCode2();
        String      recordId2   = request.getRecordId2();
        long        flags       = request.getFlags();
        SzRecordKey recordKey1  = SzRecordKey.of(dataSource1, recordId1);
        SzRecordKey recordKey2  = SzRecordKey.of(dataSource2, recordId2);
        Set<SzFlag> flagSet     = SZ_WHY_RECORDS_FLAGS.toFlagSet(flags);

        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.whyRecords(recordKey1, recordKey2, flagSet);
                
            WhyRecordsResponse response
                = WhyRecordsResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void whySearch(WhySearchRequest request,
        StreamObserver<WhySearchResponse> responseObserver) 
    {
        String      attributes  = request.getAttributes();
        long        entityId    = request.getEntityId();
        String      profile     = request.getSearchProfile();
        long        flags       = request.getFlags();
        Set<SzFlag> flagSet     = SZ_WHY_SEARCH_FLAGS.toFlagSet(flags);

        try {
            SzEngine engine = this.getEnvironment().getEngine();

            String result = engine.whySearch(attributes, entityId, profile, flagSet);
                
            WhySearchResponse response
                = WhySearchResponse.newBuilder().setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SzException e) {
            responseObserver.onError(e);
        }
    }

}

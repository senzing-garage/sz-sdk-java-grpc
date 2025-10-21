package com.senzing.sdk.grpc;

import java.util.Set;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;

import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.LinkedHashMap;

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

import com.senzing.sdk.grpc.proto.SzEngineGrpc;
import com.senzing.sdk.grpc.proto.SzEngineGrpc.SzEngineBlockingStub;
import com.senzing.sdk.grpc.proto.SzEngineProto.AddRecordRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.AddRecordResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.CountRedoRecordsRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.CountRedoRecordsResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.DeleteRecordRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.DeleteRecordResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindInterestingEntitiesByEntityIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindInterestingEntitiesByEntityIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindInterestingEntitiesByRecordIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindInterestingEntitiesByRecordIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindNetworkByEntityIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindNetworkByEntityIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindNetworkByRecordIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindNetworkByRecordIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindPathByEntityIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindPathByEntityIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindPathByRecordIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.FindPathByRecordIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetEntityByEntityIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetEntityByEntityIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetEntityByRecordIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetEntityByRecordIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetRecordPreviewRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetRecordPreviewResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetRecordRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetRecordResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetRedoRecordRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetRedoRecordResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetStatsRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetStatsResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetVirtualEntityByRecordIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.GetVirtualEntityByRecordIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.HowEntityByEntityIdRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.HowEntityByEntityIdResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.PrimeEngineRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.ProcessRedoRecordRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.ProcessRedoRecordResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.ReevaluateEntityRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.ReevaluateEntityResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.ReevaluateRecordRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.ReevaluateRecordResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.SearchByAttributesRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.SearchByAttributesResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.StreamExportCsvEntityReportRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.StreamExportCsvEntityReportResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.StreamExportJsonEntityReportRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.StreamExportJsonEntityReportResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.WhyEntitiesRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.WhyEntitiesResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.WhyRecordInEntityRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.WhyRecordInEntityResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.WhyRecordsRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.WhyRecordsResponse;
import com.senzing.sdk.grpc.proto.SzEngineProto.WhySearchRequest;
import com.senzing.sdk.grpc.proto.SzEngineProto.WhySearchResponse;

import static com.senzing.sdk.grpc.proto.SzEngineGrpc.*;
import static com.senzing.sdk.grpc.proto.SzEngineProto.*;

/**
 * The gRPC implementation of {@link SzEngine}.
 */
public class SzGrpcEngine implements SzEngine {
    /**
     * The error code for an invalid export handle.
     */
    private static final int INVALID_EXPORT_HANDLE_CODE = 3103;

    /**
     * The error message format for an invalid export handle.
     */
    private static final String INVALID_EXPORT_HANDLE_MESSAGE 
        = "Invalid Export Handle [{0}]";

    /**
     * The {@link SzGrpcEnvironment} that constructed this instance.
     */
    private SzGrpcEnvironment env = null;

    /**
     * The underlying blocking stub.
     */
    private SzEngineBlockingStub blockingStub = null;

    /**
     * The {@link Map} of {@link Long} export handle keys to 
     * {@link Iterator} values for the streamed export report.
     */
    private final Map<Long, Iterator<String>> exportReportMaps;

    /**
     * The previous export handle value to use.
     */
    private long prevExportHandle = 0L;

    /**
     * The next export handle value to use.
     */
    private long nextExportHandle = 1L;

    /**
     * Provide an {@link Iterator} over {@linK String} values that uses
     * the {@link StreamExportCsvEntityReportResponse}.
     */
    private static final class CsvExportIterator implements Iterator<String> {
        /**
         * The streaming response iterator.
         */
        private Iterator<StreamExportCsvEntityReportResponse> iter = null;

        /**
         * Constructs with the specified response iterator.
         * 
         * @param iter The response iterator.
         */
        private CsvExportIterator(Iterator<StreamExportCsvEntityReportResponse> iter) {
            this.iter = iter;
            
            // check if we have a next element to force blocking for first
            this.iter.hasNext();
        }

        /**
         * Checks if we have further export content.
         * 
         * @return <code>true</code> if there is additional export content,
         *         otherwise <code>false</code>.
         */
        @Override
        public boolean hasNext() {
            return this.iter.hasNext();
        }

        /**
         * Gets the next line of export content.
         * 
         * @return The next line of export content.
         */
        @Override
        public String next() {
            StreamExportCsvEntityReportResponse response = this.iter.next();
            return response.getResult();
        }
    }

    /**
     * Provide an {@link Iterator} over {@linK String} values that uses
     * the {@link StreamExportJsonEntityReportResponse}.
     */
    private static final class JsonExportIterator implements Iterator<String> {
        /**
         * The streaming response iterator.
         */
        private Iterator<StreamExportJsonEntityReportResponse> iter = null;

        /**
         * Constructs with the specified response iterator.
         * 
         * @param iter The response iterator.
         */
        private JsonExportIterator(Iterator<StreamExportJsonEntityReportResponse> iter) {
            this.iter = iter;

            // check if we have a next element to force blocking for first
            this.iter.hasNext();
        }
        
        /**
         * Checks if we have further export content.
         * 
         * @return <code>true</code> if there is additional export content,
         *         otherwise <code>false</code>.
         */
        @Override
        public boolean hasNext() {
            return this.iter.hasNext();
        }

        /**
         * Gets the next line of export content.
         * 
         * @return The next line of export content.
         */
        @Override
        public String next() {
            StreamExportJsonEntityReportResponse response = this.iter.next();
            return response.getResult();
        }
    }

    /**
     * Package-access constructor.
     * 
     * @param environment the {@link SzGrpcEnvironment} with which to construct.
     */
    protected SzGrpcEngine(SzGrpcEnvironment environment) {
        this.env = environment;
        
        Channel channel = this.env.getChannel();

        this.blockingStub = SzEngineGrpc.newBlockingStub(channel);

        this.exportReportMaps = new LinkedHashMap<>();
    }

    /**
     * Gets the next export handle to use.
     * 
     * @return The next export handle to use.
     */
    private long getNextExportHandle() {
        // use a fibonacci sequence just so they are not sequential
        synchronized (this.exportReportMaps) {
            long result = this.nextExportHandle + this.prevExportHandle;
            this.prevExportHandle = this.nextExportHandle;
            this.nextExportHandle = result;
            return result;
        }
    }

    /**
     * Encodes the {@link Set} of {@link Long} entity ID's as JSON.
     * The JSON is formatted as:
     * <pre>
     *   {
     *     "ENTITIES": [
     *        { "ENTITY_ID": &lt;entity_id1&gt; },
     *        { "ENTITY_ID": &lt;entity_id2&gt; },
     *        . . .
     *        { "ENTITY_ID": &lt;entity_idN&gt; }
     *     ]
     *   }
     * </pre>
     * @param entityIds The non-null {@link Set} of non-null {@link Long}
     *                  entity ID's.
     * 
     * @return The encoded JSON string of entity ID's.
     */
    protected static String encodeEntityIds(Set<Long> entityIds) {
        JsonObjectBuilder   job = Json.createObjectBuilder();
        JsonArrayBuilder    jab = Json.createArrayBuilder();

        for (Long entityId : entityIds) {
            JsonObjectBuilder job2 = Json.createObjectBuilder();
            job2.add("ENTITY_ID", entityId);
            jab.add(job2);
        }

        job.add("ENTITIES", jab);
        
        StringWriter sw = new StringWriter();
        Json.createWriter(sw).writeObject(job.build());
        return sw.toString();
    }

    /**
     * Encodes the {@link Set} of {@link SzRecordKey} instances as JSON.
     * The JSON is formatted as:
     * <pre>
     *   {
     *     "RECORDS": [
     *        {
     *          "DATA_SOURCE": "&lt;data_source1&gt;",
     *          "RECORD_ID":  "&lt;record_id1&gt;"
     *        },
     *        {
     *          "DATA_SOURCE": "&lt;data_source2&gt;",
     *          "RECORD_ID":  "&lt;record_id2&gt;"
     *        },
     *        . . .
     *        {
     *          "DATA_SOURCE": "&lt;data_sourceN&gt;",
     *          "RECORD_ID":  "&lt;record_idN&gt;"
     *        }
     *     ]
     *   }
     * </pre>
     * @param recordKeys The non-null {@link Set} of non-null
     *                   {@link SzRecordKey} instances.
     * 
     * @return The encoded JSON string of record keys.
     */
    protected static String encodeRecordKeys(Set<SzRecordKey> recordKeys) {
        JsonObjectBuilder   job = Json.createObjectBuilder();
        JsonArrayBuilder    jab = Json.createArrayBuilder();

        for (SzRecordKey recordKey : recordKeys) {
            JsonObjectBuilder job2 = Json.createObjectBuilder();
            job2.add("DATA_SOURCE", recordKey.dataSourceCode());
            job2.add("RECORD_ID", recordKey.recordId());

            jab.add(job2);
        }

        job.add("RECORDS", jab);

        StringWriter sw = new StringWriter();
        Json.createWriter(sw).writeObject(job.build());
        return sw.toString();
    }

    /**
     * Encodes the {@link Set} of {@link String} data source codes
     * as JSON.  The JSON is formatted as:
     * <pre>
     *    { "DATA_SOURCES": [
     *        "&lt;data_source_code1&gt;",
     *        "&lt;data_source_code2&gt;",
     *        . . .
     *        "&lt;data_source_codeN&gt;"
     *      ]
     *    }
     * </pre>
     * @param dataSources The {@link Set} of {@link String} data source codes.
     * 
     * @return The encoded JSON string of record keys.
     */
    protected static String encodeDataSources(Set<String> dataSources) {
        JsonObjectBuilder   job = Json.createObjectBuilder();
        JsonArrayBuilder    jab = Json.createArrayBuilder();

        for (String dataSourceCode : dataSources) {
            jab.add(dataSourceCode);
        }

        job.add("DATA_SOURCES", jab);

        StringWriter sw = new StringWriter();
        Json.createWriter(sw).writeObject(job.build());
        return sw.toString();
    }

    /**
     * Gets the underlying {@link SzEngineBlockingStub} for this instance.
     * 
     * @return The underlying {@link SzEngineBlockingStub} for this instance.
     */
    protected SzEngineBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void primeEngine() throws SzException {
        this.env.execute(() -> {
            PrimeEngineRequest request 
                = PrimeEngineRequest.newBuilder().build();
            
            return this.getBlockingStub().primeEngine(request);
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String getStats() throws SzException {
        return this.env.execute(() -> {
            GetStatsRequest request 
                = GetStatsRequest.newBuilder().build();
            
            GetStatsResponse response = this.getBlockingStub().getStats(request);

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
    public String addRecord(SzRecordKey recordKey, String recordDefinition, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzBadInputException, SzException 
    {
        return this.env.execute(() -> {
            AddRecordRequest request 
                = AddRecordRequest.newBuilder()
                    .setDataSourceCode(recordKey.dataSourceCode())
                    .setRecordId(recordKey.recordId())
                    .setRecordDefinition(recordDefinition)
                    .setFlags(SzFlag.toLong(flags)).build();
            
            AddRecordResponse response = this.getBlockingStub().addRecord(request);

            String result = response.getResult();
            return (result.length() == 0) ? null : result;
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String getRecordPreview(String recordDefinition, Set<SzFlag> flags) throws SzException {
        return this.env.execute(() -> {
            GetRecordPreviewRequest request 
                = GetRecordPreviewRequest.newBuilder()
                    .setRecordDefinition(recordDefinition)
                    .setFlags(SzFlag.toLong(flags)).build();
            
            GetRecordPreviewResponse response 
                = this.getBlockingStub().getRecordPreview(request);

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
    public String deleteRecord(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzException 
    {
        return this.env.execute(() -> {
            DeleteRecordRequest request 
                = DeleteRecordRequest.newBuilder()
                    .setDataSourceCode(recordKey.dataSourceCode())
                    .setRecordId(recordKey.recordId())
                    .setFlags(SzFlag.toLong(flags)).build();
            
            DeleteRecordResponse response
                = this.getBlockingStub().deleteRecord(request);

            String result = response.getResult();
            return (result.length() == 0) ? null : result;
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String reevaluateRecord(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzException 
    {
        return this.env.execute(() -> {
            ReevaluateRecordRequest request 
                = ReevaluateRecordRequest.newBuilder()
                    .setDataSourceCode(recordKey.dataSourceCode())
                    .setRecordId(recordKey.recordId())
                    .setFlags(SzFlag.toLong(flags)).build();
            
            ReevaluateRecordResponse response
                = this.getBlockingStub().reevaluateRecord(request);

            String result = response.getResult();
            return (result.length() == 0) ? null : result;
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String reevaluateEntity(long entityId, Set<SzFlag> flags)
            throws SzException 
    {
        return this.env.execute(() -> {
            ReevaluateEntityRequest request 
                = ReevaluateEntityRequest.newBuilder()
                    .setEntityId(entityId)
                    .setFlags(SzFlag.toLong(flags)).build();
            
            ReevaluateEntityResponse response
                = this.getBlockingStub().reevaluateEntity(request);

            String result = response.getResult();
            return (result.length() == 0) ? null : result;
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String searchByAttributes(String         attributes, 
                                     String         searchProfile,
                                     Set<SzFlag>    flags) 
            throws SzException 
    {
        return this.env.execute(() -> {
            SearchByAttributesRequest.Builder builder
                = SearchByAttributesRequest.newBuilder()
                    .setAttributes(attributes)
                    .setFlags(SzFlag.toLong(flags));

            if (searchProfile != null) {
                builder.setSearchProfile(searchProfile);
            }

            SearchByAttributesRequest request = builder.build();
            
            SearchByAttributesResponse response
                = this.getBlockingStub().searchByAttributes(request);

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
    public String whySearch(String      attributes, 
                            long        entityId,
                            String      searchProfile,
                            Set<SzFlag> flags)
            throws SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            WhySearchRequest.Builder builder
                = WhySearchRequest.newBuilder()
                    .setAttributes(attributes)
                    .setEntityId(entityId)
                    .setFlags(SzFlag.toLong(flags));
            
            if (searchProfile != null) {
                builder.setSearchProfile(searchProfile);
            }

            WhySearchRequest request = builder.build();
            
            WhySearchResponse response
                = this.getBlockingStub().whySearch(request);

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
    public String getEntity(long entityId, Set<SzFlag> flags)
            throws SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            GetEntityByEntityIdRequest request
                = GetEntityByEntityIdRequest.newBuilder()
                    .setEntityId(entityId)
                    .setFlags(SzFlag.toLong(flags)).build();
            
            GetEntityByEntityIdResponse response
                = this.getBlockingStub().getEntityByEntityId(request);
            
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
    public String getEntity(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            GetEntityByRecordIdRequest request
                = GetEntityByRecordIdRequest.newBuilder()
                    .setDataSourceCode(recordKey.dataSourceCode())
                    .setRecordId(recordKey.recordId())
                    .setFlags(SzFlag.toLong(flags)).build();

            GetEntityByRecordIdResponse response
                = this.getBlockingStub().getEntityByRecordId(request);
            
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
    public String findInterestingEntities(long entityId, Set<SzFlag> flags)
            throws SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            FindInterestingEntitiesByEntityIdRequest request
                = FindInterestingEntitiesByEntityIdRequest.newBuilder()
                    .setEntityId(entityId)
                    .setFlags(SzFlag.toLong(flags)).build();

            FindInterestingEntitiesByEntityIdResponse response
                = this.getBlockingStub().findInterestingEntitiesByEntityId(request);
            
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
    public String findInterestingEntities(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            FindInterestingEntitiesByRecordIdRequest request
                = FindInterestingEntitiesByRecordIdRequest.newBuilder()
                    .setDataSourceCode(recordKey.dataSourceCode())
                    .setRecordId(recordKey.recordId())
                    .setFlags(SzFlag.toLong(flags)).build();

            FindInterestingEntitiesByRecordIdResponse response
                = this.getBlockingStub().findInterestingEntitiesByRecordId(request);
            
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
    public String findPath(long         startEntityId,
                           long         endEntityId,
                           int          maxDegrees,
                           SzEntityIds  avoidEntityIds,
                           Set<String>  requiredDataSources,
                           Set<SzFlag>  flags)
        throws SzNotFoundException, SzUnknownDataSourceException, SzException 
    {
        return this.env.execute(() -> {
            FindPathByEntityIdRequest.Builder builder
                = FindPathByEntityIdRequest.newBuilder()
                    .setStartEntityId(startEntityId)
                    .setEndEntityId(endEntityId)
                    .setMaxDegrees(maxDegrees)
                    .setFlags(SzFlag.toLong(flags));
            if (avoidEntityIds != null) {
                builder.setAvoidEntityIds(encodeEntityIds(avoidEntityIds));
            }
            if (requiredDataSources != null) {
                builder.setRequiredDataSources(
                    encodeDataSources(requiredDataSources));
            }
            FindPathByEntityIdRequest request = builder.build();

            FindPathByEntityIdResponse response
                = this.getBlockingStub().findPathByEntityId(request);
            
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
    public String findPath(SzRecordKey  startRecordKey, 
                           SzRecordKey  endRecordKey,
                           int          maxDegrees,
                           SzRecordKeys avoidRecordKeys,
                           Set<String>  requiredDataSources,
                           Set<SzFlag>  flags)
            throws SzNotFoundException, SzUnknownDataSourceException, SzException 
    {
        return this.env.execute(() -> {
            FindPathByRecordIdRequest.Builder builder 
                = FindPathByRecordIdRequest.newBuilder()
                    .setStartDataSourceCode(startRecordKey.dataSourceCode())
                    .setStartRecordId(startRecordKey.recordId())
                    .setEndDataSourceCode(endRecordKey.dataSourceCode())
                    .setEndRecordId(endRecordKey.recordId())
                    .setMaxDegrees(maxDegrees)
                    .setFlags(SzFlag.toLong(flags));

            if (avoidRecordKeys != null) {
                builder.setAvoidRecordKeys(
                    encodeRecordKeys(avoidRecordKeys));
            }
            if (requiredDataSources != null) {
                builder.setRequiredDataSources(
                    encodeDataSources(requiredDataSources));
            }
            FindPathByRecordIdRequest request = builder.build();
            FindPathByRecordIdResponse response
                = this.getBlockingStub().findPathByRecordId(request);
            
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
    public String findNetwork(SzEntityIds   entityIds, 
                              int           maxDegrees,
                              int           buildOutDegrees,
                              int           buildOutMaxEntities,
                              Set<SzFlag>   flags)
            throws SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            FindNetworkByEntityIdRequest request
                = FindNetworkByEntityIdRequest.newBuilder()
                    .setEntityIds(encodeEntityIds(entityIds))
                    .setMaxDegrees(maxDegrees)
                    .setBuildOutDegrees(buildOutDegrees)
                    .setBuildOutMaxEntities(buildOutMaxEntities)
                    .setFlags(SzFlag.toLong(flags)).build();

            FindNetworkByEntityIdResponse response
                = this.getBlockingStub().findNetworkByEntityId(request);
            
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
    public String findNetwork(SzRecordKeys  recordKeys, 
                              int           maxDegrees,
                              int           buildOutDegrees,
                              int           buildOutMaxEntities,
                              Set<SzFlag>   flags) 
        throws SzUnknownDataSourceException, SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            FindNetworkByRecordIdRequest request
                = FindNetworkByRecordIdRequest.newBuilder()
                    .setRecordKeys(encodeRecordKeys(recordKeys))
                    .setMaxDegrees(maxDegrees)
                    .setBuildOutDegrees(buildOutDegrees)
                    .setBuildOutMaxEntities(buildOutMaxEntities)
                    .setFlags(SzFlag.toLong(flags)).build();

            FindNetworkByRecordIdResponse response
                = this.getBlockingStub().findNetworkByRecordId(request);
            
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
    public String whyRecordInEntity(SzRecordKey recordKey, Set<SzFlag> flags)
        throws SzUnknownDataSourceException, SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            WhyRecordInEntityRequest request 
                = WhyRecordInEntityRequest.newBuilder()
                    .setDataSourceCode(recordKey.dataSourceCode())
                    .setRecordId(recordKey.recordId())
                    .setFlags(SzFlag.toLong(flags)).build();
            
            WhyRecordInEntityResponse response
                = this.getBlockingStub().whyRecordInEntity(request);

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
    public String whyRecords(SzRecordKey recordKey1, 
                             SzRecordKey recordKey2, 
                             Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            WhyRecordsRequest request 
                = WhyRecordsRequest.newBuilder()
                    .setDataSourceCode1(recordKey1.dataSourceCode())
                    .setRecordId1(recordKey1.recordId())
                    .setDataSourceCode2(recordKey2.dataSourceCode())
                    .setRecordId2(recordKey2.recordId())
                    .setFlags(SzFlag.toLong(flags)).build();
            
            WhyRecordsResponse response = this.getBlockingStub().whyRecords(request);

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
    public String whyEntities(long entityId1, long entityId2, Set<SzFlag> flags)
            throws SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            WhyEntitiesRequest request 
                = WhyEntitiesRequest.newBuilder()
                    .setEntityId1(entityId1)
                    .setEntityId2(entityId2)
                    .setFlags(SzFlag.toLong(flags)).build();
            
            WhyEntitiesResponse response = this.getBlockingStub().whyEntities(request);

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
    public String howEntity(long entityId, Set<SzFlag> flags) 
        throws SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            HowEntityByEntityIdRequest request
                = HowEntityByEntityIdRequest.newBuilder()
                    .setEntityId(entityId)
                    .setFlags(SzFlag.toLong(flags)).build();
            
            HowEntityByEntityIdResponse response
                = this.getBlockingStub().howEntityByEntityId(request);
            
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
    public String getVirtualEntity(Set<SzRecordKey> recordKeys, Set<SzFlag> flags)
            throws SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            GetVirtualEntityByRecordIdRequest request
                = GetVirtualEntityByRecordIdRequest.newBuilder()
                    .setRecordKeys(encodeRecordKeys(recordKeys))
                    .setFlags(SzFlag.toLong(flags)).build();

            GetVirtualEntityByRecordIdResponse response
                = this.getBlockingStub().getVirtualEntityByRecordId(request);
            
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
    public String getRecord(SzRecordKey recordKey, Set<SzFlag> flags)
            throws SzUnknownDataSourceException, SzNotFoundException, SzException 
    {
        return this.env.execute(() -> {
            GetRecordRequest request
                = GetRecordRequest.newBuilder()
                    .setDataSourceCode(recordKey.dataSourceCode())
                    .setRecordId(recordKey.recordId())
                    .setFlags(SzFlag.toLong(flags)).build();

            GetRecordResponse response = this.getBlockingStub().getRecord(request);
            
            return response.getResult();
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}
     * using the streaming functionality.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public long exportJsonEntityReport(Set<SzFlag> flags) throws SzException {
        return this.env.execute(() -> {
            StreamExportJsonEntityReportRequest request
                = StreamExportJsonEntityReportRequest.newBuilder()
                    .setFlags(SzFlag.toLong(flags)).build();
            
            Iterator<StreamExportJsonEntityReportResponse> responseIter
                = this.getBlockingStub().streamExportJsonEntityReport(request);

            JsonExportIterator exportIter = new JsonExportIterator(responseIter);
            
            long exportHandle = 0L;
            synchronized (this.exportReportMaps) {
                exportHandle = this.getNextExportHandle();
                this.exportReportMaps.put(exportHandle, exportIter);
            }

            return exportHandle;
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}
     * using the streaming functionality.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public long exportCsvEntityReport(String csvColumnList, Set<SzFlag> flags) 
        throws SzException 
    {
        return this.env.execute(() -> {
            StreamExportCsvEntityReportRequest request
                = StreamExportCsvEntityReportRequest.newBuilder()
                    .setCsvColumnList(csvColumnList)
                    .setFlags(SzFlag.toLong(flags)).build();
            
            Iterator<StreamExportCsvEntityReportResponse> responseIter
                = this.getBlockingStub().streamExportCsvEntityReport(request);

            CsvExportIterator exportIter = new CsvExportIterator(responseIter);
            
            long exportHandle = 0L;
            synchronized (this.exportReportMaps) {
                exportHandle = this.getNextExportHandle();
                this.exportReportMaps.put(exportHandle, exportIter);
            }

            return exportHandle;
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}
     * using the streaming response.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String fetchNext(long exportHandle) throws SzException {
        return this.env.execute(() -> {
            Iterator<String> exportIter = null;
            synchronized (this.exportReportMaps) {
                exportIter = this.exportReportMaps.get(exportHandle);
            }

            // check if not found
            if (exportIter == null) {
                throw new SzException(
                    INVALID_EXPORT_HANDLE_CODE, 
                    INVALID_EXPORT_HANDLE_MESSAGE.replace(
                        "{0}", String.valueOf(exportHandle)));
            }

            // check if none remaining
            if (!exportIter.hasNext()) {
                return null;
            }
            
            // get the next line
            return exportIter.next();
        });
    }

    /**
     * Implemented to execute the operation by closing the the 
     * streaming response iterator previously obtained from the 
     * gRPC server using the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void closeExportReport(long exportHandle) throws SzException {
        this.env.execute(() -> {
            Iterator<String> exportIter = null;
            synchronized (this.exportReportMaps) {
                exportIter = this.exportReportMaps.remove(exportHandle);
            }
            if (exportIter == null) {
                throw new SzException(
                    INVALID_EXPORT_HANDLE_CODE, 
                    INVALID_EXPORT_HANDLE_MESSAGE.replace(
                        "{0}", String.valueOf(exportHandle)));
            }
            return null;
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String processRedoRecord(String redoRecord, Set<SzFlag> flags)
        throws SzException 
    {
        return this.env.execute(() -> {
            ProcessRedoRecordRequest request
                = ProcessRedoRecordRequest.newBuilder()
                    .setRedoRecord(redoRecord)
                    .setFlags(SzFlag.toLong(flags)).build();

            ProcessRedoRecordResponse response
                = this.getBlockingStub().processRedoRecord(request);
            
            String result = response.getResult();
            return (result.length() == 0) ? null : result;
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String getRedoRecord() throws SzException 
    {
        return this.env.execute(() -> {
            GetRedoRecordRequest request 
                = GetRedoRecordRequest.newBuilder().build();
            
            GetRedoRecordResponse response
                = this.getBlockingStub().getRedoRecord(request);
            
            String result = response.getResult();
            return (result.length() == 0) ? null : result;
        });
    }

    /**
     * Implemented to execute the operation over gRPC against the 
     * gRPC server from the associated {@link SzGrpcEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public long countRedoRecords() throws SzException 
    {
        return this.env.execute(() -> {
            CountRedoRecordsRequest request 
                = CountRedoRecordsRequest.newBuilder().build();
            
            CountRedoRecordsResponse response
                = this.getBlockingStub().countRedoRecords(request);
            
            return response.getResult();
        });
    }
}

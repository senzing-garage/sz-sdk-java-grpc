package com.senzing.sdk.grpc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.server.annotation.Order;
import com.senzing.sdk.SzRecordKey;
import com.senzing.datamart.reports.model.SzEntitySizeBreakdown;
import com.senzing.datamart.reports.model.SzEntitySizeCount;
import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.grpc.server.SzGrpcServer;
import com.senzing.sdk.grpc.server.SzGrpcServerOptions;
import com.senzing.datamart.ConnectionUri;
import com.senzing.datamart.ProcessingRate;
import com.senzing.datamart.SqliteUri;
import com.senzing.datamart.SzCoreSettingsUri;
import com.senzing.datamart.reports.EntitySizeReports;
import com.senzing.datamart.reports.EntitySizeReportsService;

import io.grpc.ManagedChannel;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static com.senzing.util.Quantified.Statistic;
import static com.senzing.listener.service.scheduling.AbstractSchedulingService.Stat.*;
import static com.senzing.datamart.SzReplicatorConstants.DEFAULT_CORE_SETTINGS_DATABASE_PATH;
import static com.senzing.io.IOUtilities.UTF_8;
import static com.senzing.sql.SQLUtilities.close;
import static com.senzing.datamart.reports.EntitySizeReportsService.*;

/**
 * 
 */
 @TestInstance(Lifecycle.PER_CLASS)
 @Execution(ExecutionMode.SAME_THREAD)
 @TestMethodOrder(OrderAnnotation.class)
public class DataMartIntegrationTest extends AbstractGrpcTest {
    /**
     * The data source code for the passengers data source.
     */
    public static final String PASSENGERS = "PASSENGERS";

    /**
     * The data source code for the employees data source.
     */
    public static final String EMPLOYEES = "EMPLOYEES";

    /**
     * The data source code for the VIP's data source.
     */
    public static final String VIPS = "VIPS";

    private SzGrpcEnvironment env = null;

    private SzGrpcServer server = null;

    private ManagedChannel channel = null;
    
    private long taskCount = 0;

    private long followUpCount = 0;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public SzEngine getEngine() throws SzException {
        return this.env.getEngine();
    }

    @Override
    protected SzGrpcServerOptions getServerOptions() {
        SzCoreSettingsUri coreSettingsUri 
            = new SzCoreSettingsUri(DEFAULT_CORE_SETTINGS_DATABASE_PATH);
        SzGrpcServerOptions options = super.getServerOptions();
        String settings = this.getRepoSettings();
        ConnectionUri connUri = coreSettingsUri.resolveUri(settings);
        if (connUri instanceof SqliteUri) {
            SqliteUri sqliteUri = (SqliteUri) connUri;
            File file = sqliteUri.getFile();
            file = new File(file.getParentFile(), "DataMart.db");
            connUri = new SqliteUri(file, sqliteUri.getQueryOptions());
        }
        options.setDataMartDatabaseUri(connUri);
        options.setDataMartRate(ProcessingRate.AGGRESSIVE);
        return options;
    }

    /**
     * Waits until the replicator task count increases by the specified
     * count.
     * 
     * @param taskCountGoal The number of replicator tasks counts to wait for.
     */
    public void awaitTaskCount(int taskCountGoal) {
        long startFollowUpCount = this.followUpCount;
        long lastFollowUpCount = startFollowUpCount;

        long goal = this.taskCount = taskCountGoal;
        while (this.taskCount < goal 
                || this.followUpCount == startFollowUpCount
                || lastFollowUpCount < this.followUpCount) 
        {
            Map<Statistic, Number> stats 
                = this.server.getReplicationProvider().getStatistics();
            Number count = stats.get(taskCompleteCount);
            Number followUp = stats.get(followUpCompleteCount);
   
            // the count should not be null
            if (count == null || followUp == null) {
                return;
            }

            // set the counts
            this.taskCount = count.longValue();
            
            // check if we need to wait
            if (this.taskCount < goal) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    // interrupted
                    return;
                }
            } else {
                lastFollowUpCount = this.followUpCount;
                this.followUpCount = followUp.longValue();
                
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    // interrupted
                    return;
                }
            }
        }
    }
    
    @BeforeAll
    public void initializeEnvironment() {
        this.beginTests();
        this.initializeTestEnvironment();

        this.server = this.createServer();
        
        this.channel = this.createChannel(this.server.getActivePort());

        this.env = SzGrpcEnvironment.newBuilder()
                                    .channel(this.channel)
                                    .build();
    }

    /**
     * Overridden to configure some data sources.
     */
    protected void prepareRepository() {
        String instanceName = this.getInstanceName();
        String settings     = this.getRepoSettings();

        SzCoreEnvironment env = SzCoreEnvironment.newBuilder()
                                                 .instanceName(instanceName)
                                                 .settings(settings)
                                                 .verboseLogging(false)
                                                 .build();
        try {
            SzConfigManager configMgr = env.getConfigManager();
            SzConfig config = configMgr.createConfig();

            config.registerDataSource(PASSENGERS);
            config.registerDataSource(EMPLOYEES);
            config.registerDataSource(VIPS);

            configMgr.setDefaultConfig(config.export());

        } catch (SzException e) { 
            throw new RuntimeException(e);

        } finally {
            env.destroy();
        }
    }
    
    @AfterAll
    public void teardownEnvironment() {
        try {
            if (this.env != null) {
                this.env.destroy();
                this.env = null;
            }
            if (this.server != null) {
                this.server.destroy();
                this.server = null;
            }
            if (this.channel != null) {
                this.channel.shutdown();
                this.channel = null;
            }
            this.teardownTestEnvironment();
        } finally {
            this.endTests();
        }
    }

    protected <T> T readReport(String endpoint, Class<T> type)
        throws IOException
    {
        int port = this.server.getActivePort();
        String uri = "http://127.0.0.1:" + port + "/" 
            + SzGrpcServer.DATA_MART_PREFIX + endpoint;
        
        URL url = URI.create(uri).toURL();
        StringBuilder sb = new StringBuilder();
        char[] buffer = new char[1024];
        try (InputStream is = url.openStream();
             InputStreamReader isr = new InputStreamReader(is, UTF_8)) 
        {
            for (int readCount = isr.read(buffer);
                 readCount >= 0;
                 readCount = isr.read(buffer))
            {
                sb.append(buffer, 0, readCount);
            }
        }
        String jsonText = sb.toString();
        return this.objectMapper.readValue(jsonText, type);
    }

    @Order(100)
    @Test
    public void testEntitySizeCount() {
        this.performTest(() -> {
            Connection conn = null;
            try {
                SzEngine engine = this.getEngine();

                engine.addRecord(SzRecordKey.of(PASSENGERS, "ABC123"), """
                        {
                            "NAME_FULL": "Joe Schmoe",
                            "ADDR_FULL": "101 Main Street; Las Vegas, NV 89101",
                            "PHONE_NUMBER": "702-555-1212"
                        }
                        """);
                engine.addRecord(SzRecordKey.of(EMPLOYEES, "DEF456"), """
                        {
                            "NAME_FULL": "Joe Schmoe",
                            "ADDR_FULL": "101 Main Street; Las Vegas, NV 89101",
                            "PHONE_NUMBER": "702-555-1212"
                        }
                        """);
                engine.addRecord(SzRecordKey.of(VIPS, "GHI789"), """
                        {
                            "NAME_FULL": "Jane Smith",
                            "ADDR_FULL": "555 Main Street; Las Vegas, NV 89101",
                            "PHONE_NUMBER": "702-777-1414"
                        }
                        """);
                engine.addRecord(SzRecordKey.of(VIPS, "JKL012"), """
                        {
                            "NAME_FULL": "Bob Jones",
                            "ADDR_FULL": "777 Main Street; Las Vegas, NV 89101",
                            "PHONE_NUMBER": "702-888-1515"
                        }
                        """);
                
                this.awaitTaskCount(4);
                
                conn = this.server.getReplicationProvider().getConnectionProvider().getConnection();
                
                SzEntitySizeBreakdown breakdown
                    = this.readReport(ENTITY_SIZE_BREAKDOWN_ENDPOINT,
                                 SzEntitySizeBreakdown.class);
                
                SzEntitySizeBreakdown expected = EntitySizeReports.getEntitySizeBreakdown(conn, null);

                assertEquals(expected, breakdown, "Entity size breakdown obtained over HTTP "
                        + "does not match object obtained directly from database");
                
                System.err.println();
                System.err.println("************ BREAKDOWN: " + breakdown);
                List<SzEntitySizeCount> counts = breakdown.getEntitySizeCounts();

                assertEquals(2, counts.size(), "Unexpected number of distinct entity size counts");
                SzEntitySizeCount count2 = counts.get(0);
                SzEntitySizeCount count1 = counts.get(1);
                assertEquals(2, count2.getEntitySize(), "Unexpected entity size from breakdown");
                assertEquals(1, count2.getEntityCount(), "Unexpected entity count from breakdown");
                assertEquals(1, count1.getEntitySize(), "Unexpected entity size from breakdown");
                assertEquals(2, count1.getEntityCount(), "Unexpected entity count from breakdown");
                
                count2 = EntitySizeReports.getEntitySizeCount(conn, 2, null);
                assertEquals(2, count2.getEntitySize(), "Unexpected entity size");
                assertEquals(1, count2.getEntityCount(), "Unexpected entity count");

                count1 = EntitySizeReports.getEntitySizeCount(conn, 1, null);
                assertEquals(1, count1.getEntitySize(), "Unexpected entity size from breakdown");
                assertEquals(2, count1.getEntityCount(), "Unexpected entity count from breakdown");

            } catch (Exception e) {
                e.printStackTrace();
                fail("Failed entity size count test with exception", e);
            } finally {
                conn = close(conn);
            }
        });
    }

}

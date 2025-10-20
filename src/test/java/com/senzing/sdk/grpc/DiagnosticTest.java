package com.senzing.sdk.grpc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import com.senzing.sdk.SzException;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.grpc.server.SzGrpcServer;
import com.senzing.sdk.test.StandardTestDataLoader;
import com.senzing.sdk.test.SzDiagnosticTest;
import com.senzing.sdk.test.TestDataLoader;

import io.grpc.ManagedChannel;

import com.senzing.sdk.SzDiagnostic;

import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * Unit tests for {@link SzCoreDiagnostic}.
 */
 @TestInstance(Lifecycle.PER_CLASS)
 @Execution(ExecutionMode.SAME_THREAD)
 @TestMethodOrder(OrderAnnotation.class)
 public class DiagnosticTest 
    extends AbstractGrpcTest 
    implements SzDiagnosticTest
{
    private SzGrpcEnvironment env = null;
    private SzGrpcServer server = null;
    private ManagedChannel channel = null;

    private TestData testData = new TestData();

    @Override
    public SzDiagnostic getDiagnostic() throws SzException {
        return this.env.getDiagnostic();
    }

    @Override
    public TestData getTestData() {
        return this.testData;
    }

    @BeforeAll
    public void initializeEnvironment() {
        this.beginTests();
        this.initializeTestEnvironment();

        this.server = this.createServer();
        
        this.channel = this.createChannel(this.server.getActiveGrpcPort());

        this.env = SzGrpcEnvironment.newBuilder()
                                    .channel(this.channel)
                                    .build();
    }
    
    /**
     * Overridden to load test data and extract features.
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
            TestDataLoader loader = new StandardTestDataLoader(env);
        
            this.testData.setup(loader);
        
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
}

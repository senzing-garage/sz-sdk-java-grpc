package com.senzing.sdk.grpc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.test.StandardTestConfigurator;
import com.senzing.sdk.test.SzConfigTest;

import io.grpc.ManagedChannel;

import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.grpc.server.SzGrpcServer;

import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class ConfigTest
    extends AbstractGrpcTest 
    implements SzConfigTest
{
    private SzGrpcEnvironment env = null;

    private SzGrpcServer server = null;
    
    private ManagedChannel channel = null;

    private TestData testData = new TestData();

    @Override
    public SzConfigManager getConfigManager() throws SzException {
        return this.env.getConfigManager();
    }

    @Override 
    public TestData getTestData() {
        return this.testData;
    }

    @BeforeAll
    public void initializeEnvironment() {
        this.beginTests();
        this.initializeTestEnvironment();
        String settings = this.getRepoSettings();
        
        String instanceName = this.getClass().getSimpleName();

        SzEnvironment env = SzCoreEnvironment.newBuilder()
                                             .instanceName(instanceName)
                                             .settings(settings)
                                             .verboseLogging(false)
                                             .build();

        try {
            StandardTestConfigurator configurator
                = new StandardTestConfigurator(env);

            this.testData.setup(configurator);
            
        } finally {
            env.destroy();
        }

        this.server = this.createServer();
        
        this.channel = this.createChannel(this.server.getActivePort());

        this.env = SzGrpcEnvironment.newBuilder()
                                    .channel(this.channel)
                                    .build();
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

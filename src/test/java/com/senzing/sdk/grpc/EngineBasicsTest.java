package com.senzing.sdk.grpc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.grpc.server.SzGrpcServer;
import com.senzing.sdk.test.SzEngineBasicsTest;

import io.grpc.ManagedChannel;

import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * Unit tests for {@link SzCoreDiagnostic}.
 */
@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(OrderAnnotation.class)
public class EngineBasicsTest 
    extends AbstractGrpcTest 
    implements SzEngineBasicsTest
{
    private SzGrpcEnvironment env = null;
    private SzGrpcServer server = null;
    private ManagedChannel channel = null;

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

    /**
     * Gets the {@link SzEngine} from the {@link SzCoreEnvironment}.
     * {@inheritDoc}
     * 
     * @return The {@link SzEngine} to use for this test.
     */
    public SzEngine getEngine() throws SzException {
        return this.env.getEngine();
    }
}

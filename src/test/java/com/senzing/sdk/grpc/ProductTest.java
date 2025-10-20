package com.senzing.sdk.grpc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import io.grpc.ManagedChannel;

import com.senzing.sdk.SzProduct;
import com.senzing.sdk.grpc.server.SzGrpcServer;
import com.senzing.sdk.SzException;
import com.senzing.sdk.test.SzProductTest;

@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class ProductTest 
    extends AbstractGrpcTest 
    implements SzProductTest 
{
    private SzGrpcEnvironment env = null;

    private SzGrpcServer server = null;
    
    private ManagedChannel channel = null;

    /**
     * @inheritDoc
     */
    public SzProduct getProduct() throws SzException {
        return this.env.getProduct();
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

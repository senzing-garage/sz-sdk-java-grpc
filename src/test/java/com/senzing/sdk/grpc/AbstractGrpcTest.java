package com.senzing.sdk.grpc;

import java.io.File;
import java.net.InetAddress;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.AbstractCoreTest;
import com.senzing.sdk.grpc.server.SzGrpcServer;
import com.senzing.sdk.grpc.server.SzGrpcServerConstants;
import com.senzing.sdk.grpc.server.SzGrpcServerOptions;

/**
 * Provides a base class fro the gRPC tests.
 */
public abstract class AbstractGrpcTest extends AbstractCoreTest {
    // Must be in static initializer before ANY Armeria usage
    static {
        System.setProperty("com.linecorp.armeria.transportType", "nio");
    }

    /**
     * Protected default constructor.
     */
    protected AbstractGrpcTest() {
        this(null);
    }

   /**
     * Protected constructor allowing the derived class to specify the
     * location for the entity respository.
     *
     * @param repoDirectory The directory in which to include the entity
     *                      repository.
     */
    protected AbstractGrpcTest(File repoDirectory) {
        super(repoDirectory);
    }

    /**
     * Creates a new default config and adds the specified zero or more
     * data sources to it and then returns the JSON {@link String} for that
     * config.
     * 
     * @param env The {@link SzEnvironment} to use.
     * 
     * @param dataSources The zero or more data sources to add to the config.
     * 
     * @return The JSON {@link String} that for the created config.
     */
    protected String createConfig(SzEnvironment env, String... dataSources) 
        throws SzException
    {
        SzConfig config = env.getConfigManager().createConfig();
        for (String dataSource : dataSources) {
            config.registerDataSource(dataSource);
        }
        return config.export();
    }

    /**
     * Creates and returns a new instance of {@link SzGrpcServer} using
     * the result from {@link #getServerOptions()}.  Calling this function
     * will also start the server.  Use {@link #createServer(boolean)} to
     * create the server without starting it.
     * 
     * @return The newly created {@link SzGrpcServer}.
     */
    protected SzGrpcServer createServer() {
        return createServer(true);
    }

    /**
     * Variant of {@link #createServer()} that allows you to control whether
     * or not the {@link SzGrpcServer} is started upon creation and before
     * being being returned.
     * 
     * @param startServer <code>true</code> if the returned {@link SzGrpcServer}
     *                    should be started, and <code>false</code> if it should
     *                    be created but not started.
     * 
     * @return The {@link SzGrpcServer} that was created.
     */
    protected SzGrpcServer createServer(boolean startServer) {
        return new SzGrpcServer(this.getServerOptions(), startServer);
    }

    /**
     * Gets the {@link SzGrpcServerOptions} using the results from:
     * <ul>
     *    <li>{@link #getRepoSettings()} for the core settings.</li>
     *    <li>{@link #getInstanceName()} for the core instance name.</li>
     *    <li>{@link #getCoreConcurrency()} for the core concurrency.</li>
     *    <li>{@link #getGrpcConcurrency()} for the gRPC concurrency.</li>
     *    <li>{@link #getGrpcPort()} for the gRPC port.</li>
     * </ul>
     * 
     * @return
     */
    protected SzGrpcServerOptions getServerOptions() {
        SzGrpcServerOptions options = new SzGrpcServerOptions(this.getRepoSettings());
        options.setCoreInstanceName(this.getInstanceName());
        options.setCoreConcurrency(this.getCoreConcurrency());
        options.setGrpcConcurrency(this.getGrpcConcurrency());
        options.setGrpcPort(this.getGrpcPort());
        return options;
    }

    /**
     * Gets the core concurrency to use for this test suite.
     * This returns <code>null</code> by default.
     * 
     * @return The core concurrency for this test suite.
     */
    protected Integer getCoreConcurrency() {
        return SzGrpcServerConstants.DEFAULT_CORE_CONCURRENCY;
    }

    /**
     * Gets the concurrency for the gRPC server.
     * 
     * @return The concurrency for the gRPC server.
     */
    protected int getGrpcConcurrency() {
        return SzGrpcServerConstants.DEFAULT_GRPC_CONCURRENCY;
    }

    /**
     * Gets the port number for the gRPC server.  The default implementation
     * returns zero (0) to use a random port for tests.
     * 
     * @return The port number for the gRPC server.
     */
    protected int getGrpcPort() {
        return 0;
    }

    /**
     * Creates the gRPC {@link ManagedChannel} for the loopback address
     * and the specified port number.
     * 
     * @param port The port number to connect to.
     * 
     * @return The created {@link ManagedChannel}.
     */
    protected ManagedChannel createChannel(int port) {
        return this.createChannel(InetAddress.getLoopbackAddress(), port);
    }

    /**
     * Creates the gRPC {@link ManagedChannel} for the specified
     * {@link InetAddress} and the port number.
     * 
     * @param address The {@link InetAddress} to connect to.
     * @param port The port number to connect to.
     * 
     * @return The created {@link ManagedChannel}.
     */
    protected ManagedChannel createChannel(InetAddress address, int port) {
        return ManagedChannelBuilder
            .forAddress(address.getHostAddress(), port)
            .usePlaintext()
            .build();
        
    }
}

package com.senzing.sdk.grpc.server;

import java.net.InetAddress;
import java.time.Duration;

/**
 * Utility class to provide common constants pertaining to the
 * Senzing gRPC Server.  These are factored into their own class
 * to avoid circular dependencies.
 */
public final class SzGrpcServerConstants {
    /**
     * Private default constructor.
     */
    private SzGrpcServerConstants() {
        // do nothing
    }

    /**
     * The default port for the gRPC Server ({@value}).
     */
    public static final int DEFAULT_PORT = 8261;

    /**
     * The default port as a string.
     */
    static final String DEFAULT_PORT_PARAM = String.valueOf(DEFAULT_PORT);

    /**
     * The default bind address option ({@value}) parameter value.
     */
    public static final String DEFAULT_SERVER_ADDRESS_PARAM = "loopback";
    
    /**
     * The default bind address as an {@link InetAddress}.
     */
    public static final InetAddress DEFAULT_SERVER_ADDRESS
        = InetAddress.getLoopbackAddress();

    /**
     * The default module name ({@value}).
     */
    public static final String DEFAULT_INSTANCE_NAME = "senzing-grpc-server";

    /**
     * The default core concurrency setting used by API server
     * instances if an explicit core concurrency is not provided.
     */
    public static final int DEFAULT_CORE_CONCURRENCY
        = Runtime.getRuntime().availableProcessors();

    /**
     * The default core concurrency as a string.
     */
    static final String DEFAULT_CORE_CONCURRENCY_PARAM
        = String.valueOf(DEFAULT_CORE_CONCURRENCY);

    /**
     * The default number of threads for gRPC request handling.
     */
    public static final int DEFAULT_GRPC_CONCURRENCY 
        = Runtime.getRuntime().availableProcessors() * 4;

    /**
     * The default gRPC concurrency as a string.
     */
    static final String DEFAULT_GRPC_CONCURRENCY_PARAM
        = String.valueOf(DEFAULT_GRPC_CONCURRENCY);

    /**
     * The default stats interval for logging stats. This is the default
     * minimum period of time between logging of stats. The actual interval
     * may be longer if the gRPC Server is idle or not performing activities
     * related to entity scoring (i.e.: activities that would affect stats).
     * The default is every fifteen minutes.
     */
    public static final long DEFAULT_LOG_STATS_SECONDS = 60L * 15L;

    /**
     * The default stats interval as a string.
     */
    static final String DEFAULT_LOG_STATS_SECONDS_PARAM 
        = String.valueOf(DEFAULT_LOG_STATS_SECONDS);

    /**
     * The default number of seconds to wait in between checking 
     * for changes in the configuration and automatically refreshing
     * the configuration.
     */
    public static final long DEFAULT_REFRESH_CONFIG_SECONDS
        = (Duration.ofHours(12).toMillis()) / 1000;

    /**
     * The config auto refresh period as a string.
     */
    static final String DEFAULT_REFRESH_CONFIG_SECONDS_PARAM 
        = String.valueOf(DEFAULT_REFRESH_CONFIG_SECONDS);

    /**
     * The prefix for environment variables used that are specific to the
     * Senzing REST API Server.
     */
    static final String ENV_PREFIX = "SENZING_TOOLS_";
}

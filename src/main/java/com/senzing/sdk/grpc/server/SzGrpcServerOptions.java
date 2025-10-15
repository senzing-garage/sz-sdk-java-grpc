package com.senzing.sdk.grpc.server;

import com.senzing.cmdline.CommandLineOption;
import com.senzing.util.JsonUtilities;

import javax.json.JsonObject;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static com.senzing.sdk.grpc.server.SzGrpcServerConstants.*;
import static com.senzing.sdk.grpc.server.SzGrpcServerOption.*;

/**
 * Describes the options to be set when constructing an instance of
 * {@link SzGrpcServer}.
 */
public class SzGrpcServerOptions {

    /**
     * Used to annotate methods with their associated {@link SzGrpcServerOption}.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Option {
        /**
         * Gets the {@link SzGrpcServerOption} associated with the
         * method that it annotates.
         * 
         * @return The {@link SzGrpcServerOption} associated with the
         *         method that it annotates.
         */
        SzGrpcServerOption value();
    }

    /**
     * The {@link Map} of {@link SzGrpcServerOption} keys to {@link Method}
     * values for getting the associated value.
     */
    private static final Map<SzGrpcServerOption, Method> GETTER_METHODS;

    /**
     * The {@link Map} of {@link SzGrpcServerOption} keys to {@link Method}
     * values for setting the associated value.
     */
    private static final Map<SzGrpcServerOption, Method> SETTER_METHODS;

    /**
     * The gRPC port.
     */
    private int grpcPort = DEFAULT_PORT;

    /**
     * The gRPC server address.
     */
    private InetAddress bindAddress = InetAddress.getLoopbackAddress();

    /**
     * The instance name with which to initialize the code SDK.
     */
    private String coreInstanceName = DEFAULT_INSTANCE_NAME;

    /**
     * The settings with which to initialize the code SDK.
     */
    private JsonObject coreSettings = null;

    /**
     * The config ID with which to initialize the core SDK (or null).
     */
    private Long coreConfigId = null;

    /**
     * The log level with which to initialize the core SDK.
     */
    private int coreLogLevel = 0;

    /**
     * The concurrency with which to initialize the auto core SDK.
     */
    private int coreConcurrency = DEFAULT_CORE_CONCURRENCY;

    /**
     * The concurrency with which to initialize the gRPC server.
     */
    private int grpcConcurrency = DEFAULT_GRPC_CONCURRENCY;

    /**
     * The config refresh period (in seconds) with which to initialize
     * the auto core SDK.
     */
    private Long refreshConfigSeconds = null;

    /**
     * The minimum number of seconds between logging of engine statistics.
     */
    private long logStatsSeconds = DEFAULT_LOG_STATS_SECONDS;

    /**
     * Whether or not to skip logging of repository performance on startup.
     */
    private boolean skipStartupPerf = false;

    /**
     * Whether or not to skip priming the engine on startup.
     */
    private boolean skipEnginePriming = false;

    /**
     * Constructs with the {@link Map} of {@link CommandLineOption}
     * keys to {@link Object} values.
     * 
     * @param optionMap The {@link Map} of {@link CommandLineOption}
     *                  keys to {@link Object} values.
     */
    @SuppressWarnings("rawtypes")
    protected SzGrpcServerOptions(Map<CommandLineOption, Object> optionMap) 
    {
        setOptions(this, optionMap);
    }

    /**
     * Constructs with the Senzing SDK initialization settings as a
     * {@link JsonObject}.
     *
     * @param settings The Senzing SDK initialization settings as a
     *                 {@link JsonObject}.
     */
    public SzGrpcServerOptions(JsonObject settings) {
        Objects.requireNonNull(settings,
                "JSON init parameters cannot be null");
        this.coreSettings = settings;
    }

    /**
     * Constructs with the Senzing SDK initialization settings as JSON text.
     *
     * @param settings The Senzing SDK initialization settings as JSON text.
     */
    public SzGrpcServerOptions(String settings) {
        this(JsonUtilities.parseJsonObject(settings));
    }

    /**
     * Returns the {@link JsonObject} describing the Senzing SDK
     * initialization settings.
     *
     * @return The {@link JsonObject} describing the Senzing SDK
     *         initialization settings.
     */
    public JsonObject getCoreSettings() {
        return this.coreSettings;
    }

    /**
     * Returns the gRPC port to bind to. Zero (0) is returned if binding to
     * a random available port. This is initialized to the {@linkplain
     * SzGrpcServerConstants#DEFAULT_PORT default port number} if not explicitly
     * set.
     *
     * @return The gRPC port to bind to or zero (0) if the server will bind
     *         to a random available port.
     */
    @Option(GRPC_PORT)
    public int getGrpcPort() {
        return this.grpcPort;
    }

    /**
     * Sets the gRPC port to bind to. Use zero to bind to a random port and
     * <code>null</code> to bind to the {@linkplain 
     * SzGrpcServerConstants#DEFAULT_PORT default port}.
     *
     * @param port The gRPC port to bind to, zero (0) if the server should bind
     *             to a random port and <code>null</code> if server should bind
     *             to the default port.
     *
     * @return A reference to this instance.
     */
    @Option(GRPC_PORT)
    public SzGrpcServerOptions setGrpcPort(Integer port) {
        this.grpcPort = (port != null) ? port : DEFAULT_PORT;
        return this;
    }

    /**
     * Gets the {@link InetAddress} for the address that the server will bind
     * to. If this was never set or set to <code>null</code> then the loopback 
     * address is returned.
     *
     * @return The {@link InetAddress} to which the server will bind.
     */
    @Option(BIND_ADDRESS)
    public InetAddress getBindAddress() {
        return this.bindAddress;
    }

    /**
     * Sets the {@link InetAddress} for the address that the server will bind
     * to. Set to <code>null</code> to bind to the loopback address.
     *
     * @param addr The {@link InetAddress} for the address that the server will
     *             bind, or <code>null</code> if the loopback address is to be used.
     *
     * @return A reference to this instance.
     */
    @Option(BIND_ADDRESS)
    public SzGrpcServerOptions setBindAddress(InetAddress addr) {
        this.bindAddress = addr == null 
            ? InetAddress.getLoopbackAddress() : addr;
        return this;
    }

    /**
     * Gets the number of threads that the server will create for the
     * Senzing Core SDK operations.  If the value has not {@linkplain
     * #setCoreConcurrency(Integer) explicitly set} then {@link 
     * SzGrpcServerConstants#DEFAULT_CORE_CONCURRENCY} is returned.
     *
     * @return The number of threads that the server will create for
     *         Senzing Core SDK operations.
     */
    @Option(CORE_CONCURRENCY)
    public int getCoreConcurrency() {
        return this.coreConcurrency;
    }

    /**
     * Sets the number of threads that the server will create for the
     * Senzing Core SDK operations.  Set to <code>null</code> to use the {@linkplain
     * SzGrpcServerConstants#DEFAULT_CORE_CONCURRENCY default number of threads}.
     *
     * @param concurrency The number of threads to create for Senzing Core SDK
     *                    operations, or <code>null</code> for the default number
     *                    of threads.
     *
     * @return A reference to this instance.
     */
    @Option(CORE_CONCURRENCY)
    public SzGrpcServerOptions setCoreConcurrency(Integer concurrency) {
        this.coreConcurrency = (concurrency != null)
                ? concurrency
                : DEFAULT_CORE_CONCURRENCY;
        return this;
    }

    /**
     * Gets the number of threads that the server will create for handling gRPC
     * requests. If the value has not {@linkplain #setGrpcConcurrency(Integer)
     * explicitly set} then {@link SzGrpcServerConstants#DEFAULT_GRPC_CONCURRENCY}
     * is returned.
     *
     * @return The number of threads that the server will create for handling
     *         gRPC requests.
     */
    @Option(GRPC_CONCURRENCY)
    public int getGrpcConcurrency() {
        return this.grpcConcurrency;
    }

    /**
     * Sets the number of threads that the server will create for handling gRPC
     * requests. Set to <code>null</code> to use the {@linkplain
     * SzGrpcServerConstants#DEFAULT_GRPC_CONCURRENCY default number of threads}.
     *
     * @param concurrency The number of threads to create for handling gRPC
     *                    requests, <code>null</code> for the default number
     *                    of threads.
     *
     * @return A reference to this instance.
     */
    @Option(GRPC_CONCURRENCY)
    public SzGrpcServerOptions setGrpcConcurrency(Integer concurrency) {
        this.grpcConcurrency = (concurrency != null)
                ? concurrency
                : DEFAULT_GRPC_CONCURRENCY;
        return this;
    }

    /**
     * Gets the instance name with which to initialize the core Senzing SDK
     * via {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#instanceName(String)}.
     * If <code>null</code> is returned then {@link 
     * SzGrpcServerConstants#DEFAULT_INSTANCE_NAME} is used.
     *
     * @return The instance name with which to initialize the core Senzing SDK,
     *         or <code>null</code> if {@link 
     *         SzGrpcServerConstants#DEFAULT_INSTANCE_NAME} should be used.
     */
    @Option(CORE_INSTANCE_NAME)
    public String getCoreInstanceName() {
        return this.coreInstanceName;
    }

    /**
     * Sets the instance name with which to initialize the core Senzing SDK
     * via {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#instanceName(String)}.
     * Set to <code>null</code> if the default value of {@link 
     * SzGrpcServerConstants#DEFAULT_INSTANCE_NAME} is to be used.
     *
     * @param instanceName The instance name with which to initialize the core
     *                   Senzing SDK, or <code>null</code> then the
     *                   {@link SzGrpcServerConstants#DEFAULT_INSTANCE_NAME}
     *                   should be used. 
     * 
     * @return A reference to this instance.
     */
    @Option(CORE_INSTANCE_NAME)
    public SzGrpcServerOptions setCoreInstanceName(String instanceName) {
        this.coreInstanceName = instanceName;
        return this;
    }

    /**
     * Gets the log level with which to initialize the core Senzing SDK.
     * This returns an integer, which currently translates into a boolean
     * for {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#verboseLogging(boolean)}
     * that is <code>true</code> for non-zero values and <code>false</code>
     * for zero (0).  If the verbosity has not been {@linkplain 
     * #setCoreLogLevel(int) explicitly set} then <code>false</code> is
     * returned.
     *
     * @return Gets the log level to determine how to set the verbosity for
     *         the core Senzing SDK.
     */
    @Option(CORE_LOG_LEVEL)
    public int getCoreLogLevel() {
        return this.coreLogLevel;
    }

    /**
     * Sets the log level with which to initialize the core Senzing SDK.
     * This is set as an integer, which currently translates into a boolean
     * for {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#verboseLogging(boolean)}
     * that is <code>true</code> for non-zero values and <code>false</code>
     * for zero (0).
     *
     * @param logLevel The log level to determine how to set the verbosity
     *                 for the core Senzing SDK.
     *
     * @return A reference to this instance.
     */
    @Option(CORE_LOG_LEVEL)
    public SzGrpcServerOptions setCoreLogLevel(int logLevel) {
        this.coreLogLevel = logLevel;
        return this;
    }

    /**
     * Gets the explicit configuration ID with which to initialize the core
     * Senzing SDK via {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#configId(Long)}
     * This method returns <code>null</code> if the gRPC server should use
     * the current default configuration ID from the repository.  This method
     * returns <code>null</code> if the value has not been {@linkplain 
     * #setCoreConfigurationId(Long) explicitly set}.
     *
     * @return The explicit configuration ID with which to initialize the
     *         Senzing native engine API, or <code>null</code> if the gRPC
     *         server should use the current default configuration ID from
     *         the repository.
     */
    @Option(CORE_CONFIG_ID)
    public Long getCoreConfigurationId() {
        return this.coreConfigId;
    }

    /**
     * Sets the explicit configuration ID with which to initialize the core
     * Senzing SDK via {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#configId(Long)}.
     * Set the value to <code>null</code> if the gRPC server should use the 
     * current default configuration ID from the entity repository.
     *
     * @param configId The explicit configuration ID with which to initialize
     *                 the core Senzing SDK, or <code>null</code> if the gRPC
     *                 server should use the current default configuration ID
     *                 from the repository.
     *
     * @return A reference to this instance.
     */
    @Option(CORE_CONFIG_ID)
    public SzGrpcServerOptions setCoreConfigurationId(Long configId) {
        this.coreConfigId = configId;
        return this;
    }

    /**
     * Returns the auto refresh period which is positive to indicate a number
     * of seconds to delay, zero if auto-refresh is disabled, and negative to
     * indicate that the auto refresh thread should run but refreshes will be
     * requested manually (used for testing).
     *
     * @return The auto refresh period.
     */
    @Option(REFRESH_CONFIG_SECONDS)
    public Long getRefreshConfigSeconds() {
        return this.refreshConfigSeconds;
    }

    /**
     * Sets the configuration auto refresh period. Set the value to
     * <code>null</code> if the API server should use {@link
     * SzGrpcServerConstants#DEFAULT_REFRESH_CONFIG_SECONDS}.
     *
     * @param autoRefreshPeriod The number of seconds to automatically
     *
     * @return A reference to this instance.
     */
    @Option(REFRESH_CONFIG_SECONDS)
    public SzGrpcServerOptions setRefreshConfigSeconds(Long autoRefreshPeriod) {
        this.refreshConfigSeconds = autoRefreshPeriod;
        return this;
    }

    /**
     * Gets the minimum time interval (in seconds) for logging stats. This is
     * the minimum number of seconds between logging of stats assuming the
     * gRPC Server is performing operations that will affect stats (i.e.:
     * activities pertaining to entity scoring). If the gRPC Server is idle
     * or active, but not performing entity scoring activities then stats
     * logging will be delayed until activities are performed that will affect
     * stats. If the returned interval is zero (0) then stats logging will be
     * suppressed.
     *
     * @return The number of seconds representing the minimum interval between
     *         logging of stats, or zero (0) if stats logging is suppressed.
     */
    @Option(LOG_STATS_SECONDS)
    public long getLogStatsSeconds() {
        return this.logStatsSeconds;
    }

    /**
     * Sets the minimum time interval (in seconds) for logging stats. This is
     * the minimum number of seconds between logging of stats assuming the
     * gRPC Server is performing operations that will affect stats (i.e.:
     * activities pertaining to entity scoring). If the gRPC Server is idle
     * or active, but not performing entity scoring activities then stats
     * logging will be delayed until activities are performed that will affect
     * stats. If the specified interval is zero (0) then stats logging will be
     * suppressed.
     * 
     * @param statsInterval The minimum number of seconds between logging stats,
     *                      or a non-positive number (e.g.: zero) to suppress
     *                      logging stats.
     *
     * @return A reference to this instance.
     */
    @Option(LOG_STATS_SECONDS)
    public SzGrpcServerOptions setLogStatsSeconds(long statsInterval) {
        this.logStatsSeconds = (statsInterval < 0L) ? 0L : statsInterval;
        return this;
    }

    /**
     * Checks whether or not the gRPC server should skip the performance check that
     * is performed at startup.
     *
     * @return <code>true</code> if the gRPC server should skip the performance
     *         check performed at startup, and <code>false</code> if not.
     */
    @Option(SKIP_STARTUP_PERF)
    public boolean isSkippingStartupPerformance() {
        return this.skipStartupPerf;
    }

    /**
     * Sets whether or not the gRPC server should skip the performance check that
     * is performed at startup.
     *
     * @param skipping <code>true</code> if the gRPC server should skip the
     *                 performance check performed at startup, and
     *                 <code>false</code> if not.
     *
     * @return A reference to this instance.
     */
    @Option(SKIP_STARTUP_PERF)
    public SzGrpcServerOptions setSkippingStartupPerformance(boolean skipping) {
        this.skipStartupPerf = skipping;
        return this;
    }

    /**
     * Checks whether or not the gRPC server should skip priming the engine on
     * startup.
     *
     * @return <code>true</code> if the gRPC server should skip priming the
     *         engine on startup, and <code>false</code> if not.
     */
    @Option(SKIP_ENGINE_PRIMING)
    public boolean isSkippingEnginePriming() {
        return this.skipEnginePriming;
    }

    /**
     * Sets whether or not the gRPC server should skip the priming the engine on
     * startup.
     *
     * @param skipping <code>true</code> if the gRPC server should skip priming
     *                 the engine on startup, and <code>false</code> if not.
     *
     * @return A reference to this instance.
     */
    @Option(SKIP_ENGINE_PRIMING)
    public SzGrpcServerOptions setSkippingEnginePriming(boolean skipping) {
        this.skipEnginePriming = skipping;
        return this;
    }

    /**
     * Creates a {@link Map} of {@link CommandLineOption} keys to {@link Object}
     * values for initializing an {@link SzGrpcServer} instance.
     *
     * @return The {@link Map} of {@link CommandLineOption} keys to {@link Object}
     *         values for initializing an {@link SzGrpcServer} instance
     */
    @SuppressWarnings("rawtypes")
    protected Map<CommandLineOption, Object> buildOptionsMap() 
    {
        Map<CommandLineOption, Object> map = new HashMap<>();
        GETTER_METHODS.forEach((option, method) -> {
            try {
                put(map, option, method.invoke(this));

            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);

            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(cause);
            }
        });
        return map;
    }

    /**
     * Sets the options on the specified instance using the
     * specified {@link Map} of options to values.
     * 
     * @param options The {@link SzGrpcServerOptions} on which to
     *                set the option values.
     * @param optionsMap The {@link Map} of {@link SzGrpcServerOption}
     *                   keys to {@link Object} values.
     */
    @SuppressWarnings("rawtypes")
    protected static void setOptions(
        SzGrpcServerOptions             options,
        Map<CommandLineOption, Object>  optionsMap) 
    {
        optionsMap.forEach((option, value) -> {
            Method method = SETTER_METHODS.get(option);
            if (method != null) {
                try {
                    method.invoke(options, value);

                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);

                } catch (InvocationTargetException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                }
            }
        });
    }

    /**
     * Utility method to only put non-null values in the specified {@link Map}
     * with the specified {@link SzGrpcServerOption} key and {@link Object} value.
     *
     * @param map    The {@link Map} to put the key-value pair into.
     * @param option The {@link SzGrpcServerOption} key.
     * @param value  The {@link Object} value.
     */
    @SuppressWarnings("rawtypes")
    private static void put(Map<CommandLineOption, Object>  map,
                            SzGrpcServerOption              option,
                            Object                          value) 
    {
        if (value != null) {
            map.put(option, value);
        }
    }

    static {
        Map<SzGrpcServerOption, Method> getterMap = new LinkedHashMap<>();
        Map<SzGrpcServerOption, Method> setterMap = new LinkedHashMap<>();

        Class<SzGrpcServerOptions> cls = SzGrpcServerOptions.class;
        Method[] methods = cls.getMethods();
        for (Method method : methods) {
            Option option = method.getAnnotation(Option.class);
            if (option == null) {
                continue;
            }
            // check if the setter or getter
            if ((method.getReturnType() == SzGrpcServerOptions.class)
                && (method.getParameterTypes().length == 1))
            {
                setterMap.put(option.value(), method);

            } else if ((method.getReturnType() != Void.class)
                && (method.getParameterTypes().length == 0))
            {
                getterMap.put(option.value(), method);
            }
        }

        GETTER_METHODS = Collections.unmodifiableMap(getterMap);
        SETTER_METHODS = Collections.unmodifiableMap(setterMap);
    }
}

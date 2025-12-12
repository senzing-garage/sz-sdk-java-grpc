package com.senzing.sdk.grpc.server;

import com.senzing.cmdline.CommandLineOption;
import com.senzing.datamart.ConnectionUri;
import com.senzing.datamart.PostgreSqlUri;
import com.senzing.datamart.ProcessingRate;
import com.senzing.datamart.SQLiteUri;
import com.senzing.datamart.SzCoreSettingsUri;
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
import static com.senzing.util.SzUtilities.startsWithDatabaseUriPrefix;

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
    private long refreshConfigSeconds = DEFAULT_REFRESH_CONFIG_SECONDS;

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
     * The {@link ConnectionUri} for the data mart database connection.
     */
    private ConnectionUri dataMartDatabaseUri = null;

    /**
     * The {@link ProcessingRate} to use for the data mart.
     */
    private ProcessingRate dataMartProcessingRate = null;

    /**
     * The optional core database URI to create the core settings if 
     * not provided.
     */
    private String coreDatabaseUri = null;

    /**
     * The optional base-64-encoded license string to create the core
     * settings if not provided
     */
    private String licenseStringBase64 = null;

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
     * Default constructor.
     */
    public SzGrpcServerOptions() {
        // do nothing
    }

    /**
     * Constructs with the {@link JsonObject} representing the settings
     * with which to initialize the Senzing Core SDK.
     *
     * @param settings The {@link JsonObject} representing the settings
     *                 with which to initialize the Senzing Core SDK.
     */
    public SzGrpcServerOptions(JsonObject settings) {
        Objects.requireNonNull(settings,
                "JSON init parameters cannot be null");
        this.coreSettings = settings;
    }

    /**
     * Constructs with the JSON text representing the settings
     * with which to initialize the Senzing Core SDK.
     * 
     * @param settings The JSON text representing the settings with
     *                 which to initialize the Senzing Core SDK.
     */
    public SzGrpcServerOptions(String settings) {
        this(JsonUtilities.parseJsonObject(settings));
    }

    /**
     * Gets the {@link JsonObject} representing the 
     * settings with which to initialize the Senzing Core SDK.
     *
     * @return The {@link JsonObject} representing the settings
     *         with which to initialize the Senzing Core SDK.
     */
    @Option(CORE_SETTINGS)
    public JsonObject getCoreSettings() {
        return this.coreSettings;
    }

    /**
     * Sets the {@link JsonObject} representing the settings
     * with which to initialize the Senzing Core SDK.
     *
     * @param settings The {@link JsonObject} representing the
     *                 settings with which to initialize the
     *                 Senzing Core SDK.
     * 
     * @return A reference to this instance.
     */
    @Option(CORE_SETTINGS)
    public SzGrpcServerOptions setCoreSettings(JsonObject settings) {
        if (settings != null && this.coreDatabaseUri != null) {
            throw new IllegalStateException(
                "Cannot specify a non-null core settings when a core database "
                + "URL (" + this.coreDatabaseUri + ") has already been specified: " 
                + settings);
        }
        this.coreSettings = settings;
        return this;
    }

    /**
     * Gets the the core database URI from which to create the core
     * settings to initialize the Senzing Core SDK.
     * 
     * @return The core database URI from which to create the core
     *         settings to initialize the Senzing Core SDK.
     */
    @Option(CORE_DATABASE_URI)
    public String getCoreDatabaseUri() {
        return this.coreDatabaseUri;
    }

    /**
     * Sets the the core database URI from which to create the core
     * settings to initialize the Senzing Core SDK.
     *
     * @param uri The core database URI from which to create the
     *            core settings to initialize the Senzing Core SDK.
     * 
     * @return A reference to this instance.
     * 
     * @throws IllegalArgumentException If the specified URI does not appear to
     *                                  be valid.
     * 
     * @throws IllegalStateException If the {@linkplain #setCoreSettings(JsonObject)
     *                               core settings} have also been provided.
     */
    @Option(CORE_DATABASE_URI)
    public SzGrpcServerOptions setCoreDatabaseUri(String uri) 
        throws IllegalStateException
    {
        if (uri != null && this.coreSettings != null) {
            throw new IllegalStateException(
                "Cannot specify a non-null core database URI when core settings "
                + "have already been specified: " + uri);
        }
        if (uri != null && !startsWithDatabaseUriPrefix(uri)) {
            throw new IllegalArgumentException(
                "The specified core database URI does not appear to be "
                + "a supported core database URI: " + uri);
        }
        this.coreDatabaseUri = uri;
        return this;
    }

    /**
     * Gets the base-64-encoded Senzing license string to include
     * in the core settings to initialize the Senzing Core SDK if
     * the core settings have not provided.
     * 
     * @return The base-64-encoded Senzing license string to
     *         include in the core settings to initialize the
     *         Senzing Core SDK.
     */
    @Option(LICENSE_STRING_BASE64)
    public String getLicenseStringBase64() {
        return this.licenseStringBase64;
    }

    /**
     * Sets the base-64-encoded Senzing license string to include
     * in the core settings to initialize the Senzing Core SDK if
     * the core settings have not provided.
     *
     * @param licenseString The base-64-encoded Senzing license
     *                      string to include in the core settings
     *                      to initialize the Senzing Core SDK.
     * 
     * @return A reference to this instance.
     * 
     * @throws IllegalStateException If the {@linkplain #setCoreSettings(JsonObject)
     *                               core settings} have also been provided.
     */
    @Option(LICENSE_STRING_BASE64)
    public SzGrpcServerOptions setLicenseStringBase64(String licenseString) 
        throws IllegalStateException
    {
        if (licenseString != null && this.coreSettings != null) {
            throw new IllegalStateException(
                "Cannot specify a non-null license string when core settings "
                + "have already been specified: " + licenseString);
        }
        this.licenseStringBase64 = licenseString;
        return this;
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
     * of seconds to delay, zero if configuration refresh should only occur
     * reactively (not periodically), and a negative number to indicate that
     * configuration refresh should be disabled.
     *
     * @return The auto refresh period.
     */
    @Option(REFRESH_CONFIG_SECONDS)
    public long getRefreshConfigSeconds() {
        return this.refreshConfigSeconds;
    }

    /**
     * Sets the configuration auto refresh period. Set the value to
     * <code>null</code> if the API server should use {@link
     * SzGrpcServerConstants#DEFAULT_REFRESH_CONFIG_SECONDS}.
     * Use zero (0) to indicate that the configuration should only
     * be refreshed in reaction to detecting it is out of sync 
     * after a failure and a negative integer to disable configuration
     * refresh entirely.
     *
     * @param seconds The number of seconds between periodic automatic
     *                refresh of the configuration, zero (0) to only 
     *                refresh reactively, and a negative integer to
     *                never refresh.
     *
     * @return A reference to this instance.
     */
    @Option(REFRESH_CONFIG_SECONDS)
    public SzGrpcServerOptions setRefreshConfigSeconds(Long seconds) {
        this.refreshConfigSeconds = (seconds == null)
            ? DEFAULT_REFRESH_CONFIG_SECONDS : seconds;
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
     * Gets the data mart database {@link ConnectionUri} for this instance.
     * 
     * @return The data mart database {@link ConnectionUri} for this instance,
     *         or <code>null</code> if this has not been configured.
     */
    @Option(DATA_MART_DATABASE_URI)
    public ConnectionUri getDataMartDatabaseUri() {
        return this.dataMartDatabaseUri;
    }

    /**
     * Gets the {@link ProcessingRate} that the {@link 
     * com.senzing.datamart.SzReplicator} would use to balance between
     * quickly processing messages in order to stay closely in sync
     * with the entity repository and delaying in order to batch a
     * larger number of messages and conserve system resources.
     *
     * @return The {@link ProcessingRate} for this instance.
     */
    @Option(DATA_MART_RATE)
    public ProcessingRate getProcessingRate() {
        return this.dataMartProcessingRate;
    }

    /**
     * Sets the {@link ProcessingRate} to determine how the 
     * {@link com.senzing.datamart.SzReplicator} should balance between quickly
     * processing messages in order to stay closely in sync
     * with the entity repository and delaying in order to
     * batch a larger number of messages and conserve system
     * resources.
     *
     * @param rate The {@link ProcessingRate} for the 
     *             {@link com.senzing.datamart.SzReplicator}, or <code>null</code>
     *             if {@link ProcessingRate#STANDARD} should be used.
     *
     * @return A reference to this instance.
     */
    @Option(DATA_MART_RATE)
    public SzGrpcServerOptions setDataMartRate(ProcessingRate rate) 
    {
        this.dataMartProcessingRate = (rate == null) 
            ? ProcessingRate.STANDARD : rate;
        return this;
    }

    /**
     * Sets the data mart database {@link ConnectionUri} for this instance.
     * 
     * @param uri The data mart database {@link ConnectionUri} for this
     *            instance, or <code>null</code> if this has not been configured.
     * 
     * @return A reference to this instance.
     */
    @Option(DATA_MART_DATABASE_URI)
    public SzGrpcServerOptions setDataMartDatabaseUri(ConnectionUri uri) {
        if (!((uri instanceof PostgreSqlUri)
              || (uri instanceof SQLiteUri)
              || (uri instanceof SzCoreSettingsUri)))
        {
            throw new IllegalArgumentException("Unsupported URI type: " + uri);
        }
        this.dataMartDatabaseUri = uri;
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

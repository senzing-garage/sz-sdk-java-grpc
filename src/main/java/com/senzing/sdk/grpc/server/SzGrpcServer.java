package com.senzing.sdk.grpc.server;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Date;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.senzing.cmdline.CommandLineException;
import com.senzing.cmdline.CommandLineOption;
import com.senzing.cmdline.DeprecatedOptionWarning;
import com.senzing.datamart.ConnectionUri;
import com.senzing.datamart.ProcessingRate;
import com.senzing.datamart.SQLiteUri;
import com.senzing.datamart.SzCoreSettingsUri;
import com.senzing.datamart.SzReplicationProvider;
import com.senzing.listener.communication.sql.SQLConsumer;
import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.auto.SzAutoCoreEnvironment;
import com.senzing.sql.SQLUtilities;
import com.senzing.sql.SQLiteConnector;
import com.senzing.util.JsonUtilities;

import com.google.protobuf.GeneratedMessage;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.cors.CorsService;

import static com.senzing.sdk.grpc.server.SzGrpcServerConstants.DEFAULT_BIND_ADDRESS;
import static com.senzing.sdk.grpc.server.SzGrpcServerOption.*;
import static com.senzing.util.JsonUtilities.parseJsonObject;
import static com.senzing.util.JsonUtilities.toJsonText;
import static com.senzing.util.LoggingUtilities.*;
import static com.senzing.util.SzUtilities.basicSettingsFromDatabaseUri;
import static com.senzing.util.SzUtilities.ensureSenzingSQLiteSchema;
import static com.linecorp.armeria.common.HttpMethod.*;

/**
 * The Senzing SDK gRPC server class.  This class pairs an
 * {@link SzGrpcServices} instance with an Armeria {@link Server}
 * and manages the full lifecycle.  For embedding Senzing gRPC
 * services into an existing Armeria server, use {@link SzGrpcServices}
 * directly.
 */
public class SzGrpcServer {
    // must be in static initializer before ANY Armeria usage
    static {
        System.setProperty("com.linecorp.armeria.transportType", "nio");
    }

    /**
     * The data mart path prefix for the reports URL's.
     * @deprecated Use {@link SzGrpcServices#DATA_MART_PREFIX} instead.
     */
    @Deprecated
    public static final String DATA_MART_PREFIX = SzGrpcServices.DATA_MART_PREFIX;

    /**
     * The {@link SzEnvironment} to use.
     */
    private SzEnvironment environment = null;

    /**
     * The flag indicating if this instance should manage the {@link SzEnvironment}.
     */
    private boolean manageEnv = false;

    /**
     * The {@link SzGrpcServices} that provides the composable Senzing
     * gRPC services.
     */
    private SzGrpcServices services = null;

    /**
     * The Armeria {@link ServerBuilder} for building the gRPC server.
     */
    private ServerBuilder grpcServerBuilder = null;

    /**
     * The underlying gRPC server.
     */
    private Server grpcServer = null;

    /**
     * Tracks if the gRPC server has been started.
     */
    private boolean started = false;

    /**
     * Tracks if the gRPC server has been stopped.
     */
    private boolean stopped = false;

    /**
     * Tracks if the gRPC server has been destroyed.
     */
    private boolean destroyed;

    /**
     * Creates a new instance of {@link SzAutoCoreEnvironment} using
     * the specified options.
     *
     * @param options The {@link SzGrpcServerOptions} to use.
     * @return The {@link SzAutoCoreEnvironment} that was created using the
     *         specified options.
     *
     * @throws IllegalStateException If there is already an active instance of
     *                               {@link com.senzing.sdk.core.SzCoreEnvironment}.
     */
    protected static SzAutoCoreEnvironment createSzAutoCoreEnvironment(SzGrpcServerOptions options)
        throws IllegalStateException
    {
        JsonObject coreSettings = options.getCoreSettings();
        String coreDatabaseUri = null;
        boolean bootstrapRepo = false;

        // check if we do not have core settings
        if (coreSettings == null) {
            // get the core database URI and optional license string
            coreDatabaseUri = options.getCoreDatabaseUri();
            String licenseString = options.getLicenseStringBase64();

            // check if the core database URI was provided
            if (coreDatabaseUri != null) {
                // get the basic settings
                String jsonSettings = basicSettingsFromDatabaseUri(
                        coreDatabaseUri, licenseString);

                // parse the JSON settings
                coreSettings = parseJsonObject(jsonSettings);

                // check if our database URL is SQLite
                if (coreDatabaseUri.toLowerCase().startsWith(SQLiteUri.SCHEME_PREFIX)) {
                    SQLiteUri sqliteUri = SQLiteUri.parse(coreDatabaseUri);

                    String path = (sqliteUri.isMemory())
                            ? sqliteUri.getInMemoryIdentifier()
                            : sqliteUri.getFile().toString();
                    Map<String, String> connProps = sqliteUri.getQueryOptions();

                    SQLiteConnector connector = new SQLiteConnector(path, connProps);
                    Connection conn = null;
                    try {
                        conn = connector.openConnection();

                        bootstrapRepo = ensureSenzingSQLiteSchema(conn);

                    } catch (SQLException e) {
                        logError(e, "Failed to install Senzing SQLite schema");
                        throw new IllegalStateException(
                                "Failed to install Senzing SQLite schema", e);

                    } finally {
                        // check if it is a memory database
                        if (!sqliteUri.isMemory()) {
                            // if not a memory database then close the connection
                            // NOTE: we leave it open if an in-memory database so it
                            // does not get deleted
                            SQLUtilities.close(conn);
                        }
                    }
                }
            }
        }

        // check if we have no core settings
        if (coreSettings == null) {
            throw new IllegalArgumentException(
                    "Failed to obtain core settings from gRPC server options via "
                            + "core settings or core database URL: " + options);
        }

        String settings = JsonUtilities.toJsonText(coreSettings);

        String instanceName = options.getCoreInstanceName();

        boolean verbose = (options.getCoreLogLevel() != 0);

        int concurrency = options.getCoreConcurrency();

        long refreshSeconds = options.getRefreshConfigSeconds();
        Duration duration = (refreshSeconds < 0)
                ? null
                : Duration.ofSeconds(refreshSeconds);

        SzAutoCoreEnvironment env = SzAutoCoreEnvironment.newAutoBuilder()
                .concurrency(concurrency)
                .configRefreshPeriod(duration)
                .settings(settings)
                .instanceName(instanceName)
                .verboseLogging(verbose)
                .build();

        // force initialization
        try {
            // check if we need to bootstrap the default config
            if (bootstrapRepo) {
                SzConfigManager configMgr = env.getConfigManager();
                SzConfig templateConfig = configMgr.createConfig();
                configMgr.setDefaultConfig(templateConfig.export());
            }
        } catch (SzException e) {
            throw new RuntimeException(e);
        }
        return env;
    }

    /**
     * Constructs with the specified {@link SzGrpcServerOptions}.
     * The server will be started upon construction.
     *
     * <b>NOTE:</b> This will initialize the Senzing Core SDK via
     * {@link SzAutoCoreEnvironment} and only one active instance of
     * {@link com.senzing.sdk.core.SzCoreEnvironment} is allowed in a
     * process at any given time.
     *
     * @param options The {@link SzGrpcServerOptions} for this instance.
     *
     * @throws IllegalStateException If another instance of Senzing Core SDK
     *                               is already actively initialized.
     */
    public SzGrpcServer(SzGrpcServerOptions options)
            throws IllegalStateException
    {
        this(options, true);
    }

    /**
     * Constructs with the specified {@link SzGrpcServerOptions},
     * optionally {@linkplain #start() starting} the server upon
     * construction.
     *
     * <b>NOTE:</b> This will initialize the Senzing Core SDK via
     * {@link SzAutoCoreEnvironment} and only one active instance of
     * {@link com.senzing.sdk.core.SzCoreEnvironment} is allowed in a
     * process at any given time.
     *
     * @param options     The {@link SzGrpcServerOptions} for this instance.
     *
     * @param startServer <code>true</code> if the server should be started
     *                    upon construction, otherwise <code>false</code>
     *
     * @throws IllegalStateException If another instance of Senzing Core SDK
     *                               is already actively initialized.
     */
    public SzGrpcServer(SzGrpcServerOptions options, boolean startServer) {
        this(createSzAutoCoreEnvironment(options), true, options, startServer);
    }

    /**
     * Constructs with the specified {@link SzEnvironment} and the specified
     * {@link SzGrpcServerOptions}. The server will be started upon
     * construction. This protected constructor is provided so that derived
     * classes may use an alternate {@link SzEnvironment} implementation.
     *
     * <b>NOTE:</b> Some options specified in {@link SzGrpcServerOptions}
     * pertain to the {@link SzEnvironment} used. The default implementation
     * depends on {@link #createSzAutoCoreEnvironment(SzGrpcServerOptions)}
     * to create an instance of {@link SzAutoCoreEnvironment} accordingly.
     * The onus is on the implementer of any derived class to manage how
     * an implementation with an alternate {@link SzEnvironment} will handle
     * the specified options that normally pertain to {@link SzAutoCoreEnvironment}.
     *
     * @param env         The {@link SzEnvironment} to use.
     * @param manageEnv   <code>true</code> if the constructed instance should
     *                    manage
     *                    (e.g.: destroy) the specified {@link SzEnvironment}, or
     *                    <code>false</code> if it will be managed externally.
     * @param options     The {@link SzGrpcServerOptions} for this instance.
     * @param startServer <code>true</code> if the server should be started
     *                    upon construction, otherwise <code>false</code>
     */
    protected SzGrpcServer(SzEnvironment        env,
                           boolean              manageEnv,
                           SzGrpcServerOptions  options,
                           boolean              startServer)
    {
        int concurrency = options.getGrpcConcurrency();
        int port = options.getGrpcPort();
        InetAddress bindAddress = options.getBindAddress();

        // set the default values for any unspecified options
        if (bindAddress == null) {
            bindAddress = DEFAULT_BIND_ADDRESS;
        }

        // set the environment
        this.environment = env;
        this.manageEnv = manageEnv;

        // resolve the data mart URI if configured
        ConnectionUri dataMartUri = options.getDataMartDatabaseUri();
        ConnectionUri resolvedUri = null;
        ProcessingRate processingRate = null;
        if (dataMartUri != null) {
            processingRate = options.getProcessingRate();
            // check if we have an SzCoreSettingsUri
            if (dataMartUri instanceof SzCoreSettingsUri) {
                // get the core settings
                JsonObject coreSettings = options.getCoreSettings();
                if (coreSettings == null) {
                    throw new IllegalArgumentException(
                            "Cannot specify an " + dataMartUri.getClass().getSimpleName()
                                    + " URI (" + dataMartUri.toString() + ") if the core settings "
                                    + "have not been provided.");
                }
                SzCoreSettingsUri coreSettingsUri = (SzCoreSettingsUri) dataMartUri;
                resolvedUri = coreSettingsUri.resolveUri(coreSettings);
                if (resolvedUri == null) {
                    throw new IllegalArgumentException(
                            "Unable to resolve " + dataMartUri + " Data Mart URI using "
                                    + "the provided core settings: "
                                    + toJsonText(coreSettings, true));
                }
            } else {
                resolvedUri = dataMartUri;
            }
        }

        // create the composable services
        this.services = new SzGrpcServices(env, resolvedUri, processingRate);

        // create a CORS decorator if we need to decorate the server
        // NOTE: we decorate the ENTIRE server rather than the GrpcService or
        // AnnotatedService individually, because redirects (especially regarding
        // paths ending "/") can lead to one path supporting CORS while the other
        // does not. By using the decorator on the whole server, then all paths
        // support CORS.
        Function<? super HttpService, ? extends HttpService> corsDecorator = null;

        List<String> allowedOrigins = options.getAllowedOrigins();
        if (allowedOrigins != null && allowedOrigins.size() > 0) {
            if (allowedOrigins.contains("*")) {
                corsDecorator = CorsService.builderForAnyOrigin()
                        .allowRequestMethods(GET, HEAD, POST, PUT, DELETE, OPTIONS)
                        .allowAllRequestHeaders(true)
                        .exposeHeaders("*")
                        .maxAge(3600)
                        .newDecorator();
            } else {
                corsDecorator = CorsService.builder(allowedOrigins)
                        .allowRequestMethods(GET, HEAD, POST, PUT, DELETE, OPTIONS)
                        .allowAllRequestHeaders(true)
                        .allowCredentials()
                        .exposeHeaders("*")
                        .maxAge(3600)
                        .newDecorator();
            }
        }

        // create the server builder
        this.grpcServerBuilder = Server.builder()
                .http(new InetSocketAddress(bindAddress, port))
                .blockingTaskExecutor(concurrency);

        // configure Senzing services onto the server builder
        this.services.configureServer(this.grpcServerBuilder);

        // decorate with CORS if configured
        if (corsDecorator != null) {
            this.grpcServerBuilder.decoratorUnder("/", corsDecorator);
        }

        // optionally, start the server
        if (startServer) {
            // build the server
            this.grpcServer = this.grpcServerBuilder.build();

            this.services.start();
            this.grpcServer.start().join();
            this.started = true;
        }
    }

    /**
     * Gets the {@link SzGrpcServices} used by this instance.
     * This allows callers to access the composable Senzing services
     * independently of the Armeria server lifecycle.
     *
     * @return The {@link SzGrpcServices} used by this instance.
     */
    protected SzGrpcServices getServices() {
        return this.services;
    }

    /**
     * Gets the {@link SzReplicationProvider} for this instance
     * if the data mart has been enabled. This returns
     * <code>null</code> if the data mart is not enabled.
     *
     * @return The {@link SzReplicationProvider} for this instance,
     *         or <code>null</code> if data mart replication is not
     *         enabled.
     */
    public SzReplicationProvider getReplicationProvider() {
        return this.services.getReplicationProvider();
    }

    /**
     * Gets the {@link SQLConsumer.MessageQueue} for enqueuing INFO
     * messages for consumption by the data mart. This returns
     * <code>null</code> if the data mart is not enabled.
     *
     * @return The {@link SQLConsumer.MessageQueue} for enqueuing INFO
     *         messages for consumption by the data mart, or
     *         <code>null</code> if the data mart is not enabled.
     *
     */
    public SQLConsumer.MessageQueue getDataMartMessageQueue() {
        return this.services.getDataMartMessageQueue();
    }

    /**
     * Gets the port number on which this {@link SzGrpcServer} is
     * actively listening.
     *
     * @return The port number on which this {@link SzGrpcServer} is
     *         actively listening.
     *
     * @throws IllegalStateException If this {@link SzGrpcServer} has
     *                               not been started or has been destroyed
     *                               and is <b>not</b> actively listening
     *                               on a port.
     */
    public synchronized int getActivePort() {
        if (this.destroyed || this.stopped || !this.started) {
            throw new IllegalStateException(
                    "There is no active port because the server is not "
                            + "currently running");
        }
        return this.grpcServer.activeLocalPort();
    }

    /**
     * Starts the server so it begins servicing requests.
     * If the server has previously been started then this method has no effect.
     *
     * @throws IllegalStateException If the server has already been
     *                               {@linkplain #destroy()
     *                               destroyed}.
     */
    public synchronized void start() throws IllegalStateException {
        if (this.destroyed) {
            throw new IllegalStateException(
                    "This instance has already been destroyed");
        }

        if (!this.started) {
            if (this.grpcServer == null) {
                this.grpcServer = this.grpcServerBuilder.build();
            }

            this.services.start();
            this.grpcServer.start().join();
            this.started = true;
        }
    }

    /**
     * Destroys this instance.
     *
     * This will stop the server (if running and it has not already been stopped)
     * and will usually {@linkplain SzEnvironment#destroy() destroy} the
     * {@link SzEnvironment} (assuming it is not being managed externally).
     *
     */
    public synchronized void destroy() {
        if (this.destroyed) {
            return;
        }
        if (this.started && !this.stopped) {
            this.grpcServer.stop().join(); // stop incoming requests first
            this.services.destroy(); // destroy the services (data mart, etc.)
            this.stopped = true;
        }
        if (this.manageEnv) {
            this.environment.destroy();
        }
        this.destroyed = true;
        this.notifyAll();
    }

    /**
     * Gets the {@link SzEnvironment} used by this instance.
     *
     * The returned instance is a {@link java.lang.reflect.Proxy} that
     * will not allow the caller to invoke {@link SzEnvironment#destroy()}.
     *
     * @return The {@link SzEnvironment} used by this instance.
     */
    public synchronized SzEnvironment getEnvironment() {
        return this.services.getEnvironment();
    }

    /**
     * Checks if the gRPC server is running. This returns <code>true</code>
     * if and only if the instance was previously {@linkplain #start() started}
     * (which may happen when constructed) and has not been destroyed.
     *
     * @return <code>true</code> if the gRPC server is running, otherwise
     *         <code>false</code>.
     */
    public synchronized boolean isRunning() {
        return (this.started && !this.stopped && !this.destroyed);
    }

    /**
     * Checks if this instance has been destroyed.
     *
     * @return <code>true</code> if this instance has been destroyed,
     *         otherwise <code>false</code>.
     */
    public synchronized boolean isDestroyed() {
        return this.destroyed;
    }

    /**
     * Main function for command-line execution.
     *
     * @param args The command-line arguments.
     */
    @SuppressWarnings("rawtypes")
    public static void main(String[] args) {
        Map<CommandLineOption, Object> options = null;
        List<DeprecatedOptionWarning> warnings = new LinkedList<>();
        JsonObjectBuilder startupBuilder = Json.createObjectBuilder();
        try {
            startupBuilder.add("message", "Startup Options");

            options = parseCommandLine(args, warnings, startupBuilder);

            for (DeprecatedOptionWarning warning : warnings) {
                System.out.println(warning);
                System.out.println();
            }

        } catch (CommandLineException e) {
            System.out.println(e.getMessage());

            System.err.println();
            System.err.println(
                    "Try the " + HELP.getCommandLineFlag() + " option for help.");
            System.err.println();
            System.exit(1);

        } catch (Exception e) {
            if (!isLastLoggedException(e)) {
                logError(e, "Failed to parse command-line arguments");
            }
            System.exit(1);
        }

        // check for the help option
        if (options.containsKey(HELP)) {
            System.out.println(getUsageMessage());
            System.exit(0);
        }

        // check for the version option
        if (options.containsKey(VERSION)) {
            System.out.println();
            System.out.println(getVersionMessage());
            System.exit(0);
        }

        System.out.println("os.arch        = " + System.getProperty("os.arch"));
        System.out.println("os.name        = " + System.getProperty("os.name"));
        System.out.println("user.dir       = " + System.getProperty("user.dir"));
        System.out.println("user.home      = " + System.getProperty("user.home"));
        System.out.println("java.io.tmpdir = " + System.getProperty("java.io.tmpdir"));

        System.out.println(
                "[" + (new Date()) + "] Senzing gRPC Server: "
                        + JsonUtilities.toJsonText(startupBuilder));

        SzGrpcServer server = null;
        try {
            SzGrpcServerOptions serverOptions = new SzGrpcServerOptions(options);

            server = new SzGrpcServer(serverOptions);

        } catch (Exception e) {
            exitOnError(e);
        }

        // make a final reference to the server
        final SzGrpcServer finalServer = server;

        // make sure we cleanup if exiting by CTRL-C or due to an exception
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // IMPORTANT: make sure to destroy the server
            finalServer.destroy();
        }));

        // block until the server is destroyed (e.g., via shutdown hook)
        try {
            synchronized (finalServer) {
                while (!finalServer.isDestroyed()) {
                    finalServer.wait();
                }
            }
        } catch (InterruptedException e) {
            logWarning("WARNING: Interrupted while waiting for server shutdown.");
        }
    }

    /**
     * Parses the {@link SzGrpcServer} command line arguments and produces a
     * {@link Map} of {@link CommandLineOption} keys to {@link Object} command
     * line values.
     *
     * @param args                The arguments to parse.
     * @param deprecationWarnings The {@link List} of deprecation warnings to
     *                            populate.
     * @param startupBuilder      The {@link JsonObjectBuilder} to track all the
     *                            startup options and how they were specified.
     * @return The {@link Map} describing the command-line arguments.
     * @throws CommandLineException If a command-line parsing failure occurs.
     */
    @SuppressWarnings("rawtypes")
    protected static Map<CommandLineOption, Object> parseCommandLine(
            String[] args,
            List<DeprecatedOptionWarning> deprecationWarnings,
            JsonObjectBuilder startupBuilder)
            throws CommandLineException {
        // parse and process the command-line
        Map<CommandLineOption, Object> result = SzGrpcServerOption.parseCommandLine(
                args, deprecationWarnings, startupBuilder);

        // return the result
        return result;
    }

    /**
     * Exit on error after reporting the specified {@link Throwable}.
     *
     * @param t The {@link Throwable} to report.
     */
    private static void exitOnError(Throwable t) {
        logError(t, "Exiting due to error");
        System.exit(1);
    }

    /**
     * Gets the usage message to report on the command-line.
     *
     * @return The usage message to report on the command line.
     */
    public static String getUsageMessage() {
        return "[USAGE STRING HERE]";
    }

    /**
     * Gets the version message to report on the command-line.
     *
     * @return The version message to report on the command line.
     */
    public static String getVersionMessage() {
        return "[VERSION STRING HERE]";
    }

    /**
     * Gets a {@link String} field value from a message that may be
     * absent. If the field value is absent then <code>null</code>
     * is returned, otherwise the field value is returned.
     *
     * @param message   The {@link GeneratedMessage} from which the
     *                  value is being extracted.
     * @param fieldName The name of the field for which the value
     *                  is being extracted.
     * @return The value of the field or <code>null</code> if the value
     *         has not been explicitly set.
     * @deprecated Use {@link SzGrpcServices#getString(GeneratedMessage, String)} instead.
     */
    @Deprecated
    public static String getString(GeneratedMessage message, String fieldName) {
        return SzGrpcServices.getString(message, fieldName);
    }
}

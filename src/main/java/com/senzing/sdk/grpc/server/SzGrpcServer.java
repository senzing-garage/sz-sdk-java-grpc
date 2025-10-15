package com.senzing.sdk.grpc.server;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Date;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.senzing.cmdline.CommandLineException;
import com.senzing.cmdline.CommandLineOption;
import com.senzing.cmdline.DeprecatedOptionWarning;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzProduct;
import com.senzing.sdk.core.auto.SzAutoCoreEnvironment;
import com.senzing.util.JsonUtilities;
import com.senzing.util.LoggingUtilities;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.grpc.GrpcService;

import static com.senzing.sdk.grpc.server.SzGrpcServerConstants.DEFAULT_BIND_ADDRESS;
import static com.senzing.sdk.grpc.server.SzGrpcServerConstants.DEFAULT_GRPC_CONCURRENCY;
import static com.senzing.sdk.grpc.server.SzGrpcServerConstants.DEFAULT_PORT;
import static com.senzing.sdk.grpc.server.SzGrpcServerOption.*;
import static com.senzing.util.LoggingUtilities.*;


/**
 * The Senzing SDK gRPC server class.
 */
public class SzGrpcServer {
    /**
     * The number of milliseconds to provide advance warning of an expiring
     * license.
     */
    private static final long EXPIRATION_WARNING_MILLIS
        = Duration.ofDays(30).toMillis();
    
    /**
     * The {@link SzEnvironment} to use.
     */
    private SzEnvironment environment = null;

    /**
     * The proxied {@link SzEnvironment} to prevent calling of
     * {@link #destroy()}.
     */
    private SzEnvironment proxyEnvironment = null;

    /**
     * The flag indicating if this instance should manage the {@link SzEnvironment}.
     */
    private boolean manageEnv = false;

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

    private static final DateFormat DATE_FORMAT
        = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.UK);

    /**
     * Creates a new instance of {@link SzAutoCoreEnvironment} using 
     * the specified options.
     * 
     * @param options The {@link SzGrpcServerOptions} to use.
     * @return The {@link SzAutoCoreEnvironment} that was created using the
     *         specified options.
     * 
     * @throws SzException If a failure occurs in initialization.
     * @throws IllegalStateException If there is already an active instance of
     *                               {@link com.senzing.sdk.core.SzCoreEnvironment}.
     */
    protected static SzAutoCoreEnvironment createSzAutoCoreEnvironment(
        SzGrpcServerOptions options)
        throws SzException, IllegalStateException
    {
        String settings = JsonUtilities.toJsonText(options.getCoreSettings());

        String instanceName = options.getCoreInstanceName();
        
        boolean verbose = (options.getCoreLogLevel() != 0);
        
        int concurrency = options.getCoreConcurrency();
        
        Duration duration = Duration.ofSeconds(options.getRefreshConfigSeconds());

        return SzAutoCoreEnvironment.newAutoBuilder()
                .concurrency(concurrency)
                .configRefreshPeriod(duration)
                .settings(settings)
                .instanceName(instanceName)
                .verboseLogging(verbose)
                .build();
    }

    /**
     * Internal class to prevent calling {@link SzEnvironment#destroy()} 
     * on the {@link SzEnvironment} returned from 
     * {@link SzGrpcServer#getEnvironment()}.
     */
    protected class EnvironmentHandler implements InvocationHandler {
        /**
         * Default constructor.
         */
        public EnvironmentHandler() {
            // do nothing
        }

        /**
         * A constant for the {@link SzEnvironment#destroy()} method.
         */
        private static final Method DESTROY_METHOD;
        static {
            Method method = null;
            try {
                method = SzEnvironment.class.getMethod("destroy");
            } catch (NoSuchMethodException e) {
                throw new ExceptionInInitializerError(e);
            }
            DESTROY_METHOD = method;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable 
        {
            if (method.equals(DESTROY_METHOD)) {
                throw new UnsupportedOperationException(
                    "Destroy the SzGrpcServer in order to destroy "
                    + "the SzEnvironment.  This operation is not supported.");
            }
            try {
                return method.invoke(SzGrpcServer.this, args);

            } catch (InvocationTargetException e) {
                throw e.getCause();

            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
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
     * @throws SzException If a failure occurs in Senzing initialization.
     * 
     * @throws IllegalStateException If another instance of Senzing Core SDK
     *                               is already actively initialized.
     */
    public SzGrpcServer(SzGrpcServerOptions options)
        throws SzException, IllegalStateException
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
     * @param options The {@link SzGrpcServerOptions} for this instance.
     * 
     * @param startServer <code>true</code> if the server should be started
     *                    upon construction, otherwise <code>false</code>
     * 
     * @throws SzException If a failure occurs in Senzing initialization.
     * 
     * @throws IllegalStateException If another instance of Senzing Core SDK
     *                               is already actively initialized.
     */
    public SzGrpcServer(SzGrpcServerOptions options, boolean startServer)
        throws SzException
    {
        this(createSzAutoCoreEnvironment(options), true, options, startServer);
    }

    /**
     * Constructs with the specified {@link SzEnvironment} and the specified
     * {@link SzGrpcServerOptions}.  The server will be started upon 
     * construction.  This protected constructor is provided so that derived
     * classes may use an alternate {@link SzEnvironment} implementation.
     * 
     * <b>NOTE:</b> Some options specified in {@link SzGrpcServerOptions} 
     * pertain to the {@link SzEnvironment} used.  The default implementation
     * depends on {@link #createSzAutoCoreEnvironment(SzGrpcServerOptions)} 
     * to create an instance of {@link SzAutoCoreEnvironment} accordingly.
     * The onus is on the implementer of any derived class to manage how
     * an implementation with an alternate {@link SzEnvironment} will handle
     * the specified options that normally pertain to {@link SzAutoCoreEnvironment}.
     * 
     * @param env The {@link SzEnvironment} to use.
     * @param manageEnv <code>true</code> if the constructed instance should manage
     *                  (e.g.: destroy) the specified {@link SzEnvironment}, or
     *                  <code>false</code> if it will be managed externally.
     * @param options The {@link SzGrpcServerOptions} for this instance.
     * @param startServer <code>true</code> if the server should be started
     *                    upon construction, otherwise <code>false</code>
     * @throws SzException If a Senzing failure occurs.
     */
    protected SzGrpcServer(SzEnvironment        env,
                           boolean              manageEnv,
                           SzGrpcServerOptions  options, 
                           boolean              startServer)
        throws SzException
    {
        this(env, true, options.buildOptionsMap(), startServer);
    }

    /**
     * Constructs with the specified {@link SzEnvironment} and the specified
     * command-line options
     * {@link SzGrpcServerOptions}.  The server will be started upon 
     * construction.  This protected constructor is provided so that derived
     * classes may use an alternate {@link SzEnvironment} implementation.
     * 
     * <b>NOTE:</b> Some options specified in {@link SzGrpcServerOptions} 
     * pertain to the {@link SzEnvironment} used.  The default implementation
     * depends on {@link #createSzAutoCoreEnvironment(SzGrpcServerOptions)} 
     * to create an instance of {@link SzAutoCoreEnvironment} accordingly.
     * The onus is on the implementer of any derived class to manage how
     * an implementation with an alternate {@link SzEnvironment} will handle
     * the specified options that normally pertain to {@link SzAutoCoreEnvironment}.
     * 
     * @param env The {@link SzEnvironment} to use.
     * @param manageEnv <code>true</code> if the constructed instance should manage
     *                  (e.g.: destroy) the specified {@link SzEnvironment}, or
     *                  <code>false</code> if it will be managed externally.
     * @param options The {@link SzGrpcServerOptions} for this instance.
     * @param startServer <code>true</code> if the server should be started
     *                    upon construction, otherwise <code>false</code>
     * @throws SzException If a Senzing failure occurs.
     */
    @SuppressWarnings("rawtypes")
    protected SzGrpcServer(SzEnvironment                    env,
                           boolean                          manageEnv,
                           Map<CommandLineOption, Object>   options, 
                           boolean                          startServer)
        throws SzException
    {
        Integer     concurrency = (Integer) options.get(GRPC_CONCURRENCY);
        Integer     port        = (Integer) options.get(GRPC_PORT);
        InetAddress bindAddress = (InetAddress) options.get(BIND_ADDRESS);
        
        // set the default values for any unspecified options
        if (concurrency == null) {
            concurrency = DEFAULT_GRPC_CONCURRENCY;
        }
        if (port == null) {
            port = DEFAULT_PORT;
        }
        if (bindAddress == null) {
            bindAddress = DEFAULT_BIND_ADDRESS;
        }

        // set the environment
        this.environment = env;
        this.manageEnv   = manageEnv;

        // proxy the environment
        ClassLoader classLoader = SzGrpcServer.class.getClassLoader();
        Class<?>[] interfaces = { SzEnvironment.class };
        this.proxyEnvironment = (SzEnvironment) Proxy.newProxyInstance(
            classLoader, interfaces, new EnvironmentHandler());

        // build the server
        this.grpcServer = Server.builder()
            .http(new InetSocketAddress(bindAddress, port))
            .blockingTaskExecutor(concurrency)
            .service(GrpcService.builder()
                .useBlockingTaskExecutor(true)
                .addService(new SzGrpcProductImpl(this))
                .addService(new SzGrpcConfigImpl(this))
                .addService(new SzGrpcConfigManagerImpl(this))
                .addService(new SzGrpcDiagnosticImpl(this))
                .addService(new SzGrpcEngineImpl(this))
                .build())
            .build();

        // optionally, start the server
        if (startServer) {
            this.grpcServer.start();
            this.started = true;
        }
    }

    /**
     * Starts the server so it begins servicing requests.
     * If the server has previously been started then this method has no effect.
     * 
     * @throws IllegalStateException If the server has already been {@linkplain #destroy()
     *                               destroyed}.
     */
    public synchronized void start() throws IllegalStateException {
        if (this.destroyed) {
            throw new IllegalStateException(
                    "This instance has already been destroyed");
        }
        if (!this.started) {
            this.grpcServer.start();
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
        if (this.destroyed) return;
        if (this.started && !this.stopped) {
            this.grpcServer.stop().join();
            this.stopped = true;
        }
        if (this.manageEnv) {
            this.environment.destroy();
        }
        this.destroyed = true;
    }

    /**
     * Gets the {@link SzEnvironment} used by this instance.
     * 
     * The returned instance is a {@link Proxy} that will not allow 
     * the caller to invoke {@link SzEnvironment#destroy()}.
     * 
     * @return The {@link SzEnvironment} used by this instance.
     */
    public synchronized SzEnvironment getEnvironment() {
        return this.proxyEnvironment;
    }

    /**
     * Checks if the gRPC server is running.  This returns <code>true</code>
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
        Map<CommandLineOption, Object>  options         = null;
        List<DeprecatedOptionWarning>   warnings        = new LinkedList<>();
        JsonObjectBuilder               startupBuilder  = Json.createObjectBuilder();
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
                System.err.println();
                System.err.println(e.getMessage());
                System.err.println();
                System.err.println(LoggingUtilities.formatStackTrace(e.getStackTrace()));
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

        final DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDate(FormatStyle.LONG)
            .withZone(ZoneId.systemDefault());

        // create a thread to monitor the license expiration date
        Thread thread = new Thread(() -> {
            while (!finalServer.isDestroyed()) {
                try {
                    SzProduct product = finalServer.getEnvironment().getProduct();

                    String license = product.getLicense();

                    JsonObject jsonObject = JsonUtilities.parseJsonObject(license);

                    String dateText = JsonUtilities.getString(jsonObject, "expireDate");

                    Date expireDate = (dateText == null) ? null : DATE_FORMAT.parse(dateText);

                    if (expireDate != null) {
                        long now = System.currentTimeMillis();

                        Instant expiration = expireDate.toInstant();

                        long diff = expireDate.getTime() - now;

                        // check if within the expiration warning period
                        if (diff < 0L) {
                            System.err.println(
                                "WARNING: License expired -- was valid through "
                                    + formatter.format(expiration) + ".");

                        } else if (diff < EXPIRATION_WARNING_MILLIS) {
                            System.err.println(
                                "WARNING: License expiring soon -- valid through "
                                    + formatter.format(expiration) + ".");
                        }
                    }
                    try {
                        synchronized (finalServer) {
                            // sleep for six hours
                            finalServer.wait(Duration.ofHours(6).toMillis());
                        }
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }

                } catch (Exception failure) {
                    break;
                }
            }
        });
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            System.err.println("WARNING: Interrupted while joining thread before exit.");
        }
    }

    /**
     * Parses the {@link SzGrpcServer} command line arguments and produces a
     * {@link Map} of {@link CommandLineOption} keys to {@link Object} command
     * line values.
     *
     * @param args The arguments to parse.
     * @param deprecationWarnings The {@link List} of deprecation warnings to 
     *                            populate.
     * @param startupBuilder The {@link JsonObjectBuilder} to track all the
     *                       startup options and how they were specified.
     * @return The {@link Map} describing the command-line arguments.
     * @throws CommandLineException If a command-line parsing failure occurs.
     */
    @SuppressWarnings("rawtypes")
    protected static Map<CommandLineOption, Object> parseCommandLine(
            String[]                        args, 
            List<DeprecatedOptionWarning>   deprecationWarnings,
            JsonObjectBuilder               startupBuilder)
        throws CommandLineException 
    {
        // parse and process the command-line
        Map<CommandLineOption, Object> result 
            = SzGrpcServerOption.parseCommandLine(
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
        System.err.println(t.getMessage());
        System.err.println(LoggingUtilities.formatStackTrace(t.getStackTrace()));
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
}

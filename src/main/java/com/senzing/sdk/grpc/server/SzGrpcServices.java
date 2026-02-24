package com.senzing.sdk.grpc.server;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.linecorp.armeria.server.AnnotatedServiceBindingBuilder;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.annotation.JacksonRequestConverterFunction;
import com.linecorp.armeria.server.annotation.JacksonResponseConverterFunction;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.senzing.datamart.ConnectionUri;
import com.senzing.datamart.ProcessingRate;
import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.SzReplicator;
import com.senzing.datamart.SzReplicatorOptions;
import com.senzing.datamart.reports.DataMartReportsServices;
import com.senzing.listener.communication.sql.SQLConsumer;
import com.senzing.sdk.SzProduct;
import com.senzing.sdk.SzBadInputException;
import com.senzing.sdk.SzConfigurationException;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzLicenseException;
import com.senzing.sdk.SzNotFoundException;
import com.senzing.sdk.SzNotInitializedException;
import com.senzing.sdk.SzReplaceConflictException;
import com.senzing.sdk.SzRetryTimeoutExceededException;
import com.senzing.sdk.SzRetryableException;
import com.senzing.sdk.SzUnknownDataSourceException;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.core.SzCoreUtilities;
import com.senzing.util.JsonUtilities;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import static com.senzing.reflect.ReflectionUtilities.restrictedProxy;
import static com.senzing.sdk.grpc.SzGrpcEnvironment.*;
import static com.senzing.util.JsonUtilities.toJsonText;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Provides composable Senzing gRPC services that can be added to any
 * Armeria {@link ServerBuilder}. This allows Senzing gRPC functionality
 * to be embedded into an existing Armeria server alongside other services.
 *
 * <p>For standalone usage, see {@link SzGrpcServer} which pairs this
 * class with an Armeria {@link com.linecorp.armeria.server.Server} and
 * manages the full lifecycle.</p>
 *
 * <p><b>NOTE:</b> The {@link GrpcService} built by this class is
 * configured with {@code useBlockingTaskExecutor(true)}, so the caller
 * should configure an appropriate
 * {@link ServerBuilder#blockingTaskExecutor(int)} on their
 * {@link ServerBuilder}.</p>
 */
public class SzGrpcServices {

    /**
     * The data mart path prefix for the reports URL's.
     */
    public static final String DATA_MART_PREFIX = "/data-mart";

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

    /**
     * Provides a mapping of exception class types to {@link Status}
     * values.
     */
    private static final Map<Class<?>, Status> STATUS_MAP;

    static {
        Map<Class<?>, Status> map = new LinkedHashMap<>();
        map.put(UnsupportedOperationException.class, Status.UNIMPLEMENTED);
        map.put(NullPointerException.class, Status.INTERNAL);
        map.put(SzNotFoundException.class, Status.NOT_FOUND);
        map.put(SzUnknownDataSourceException.class, Status.NOT_FOUND);
        map.put(IllegalArgumentException.class, Status.INVALID_ARGUMENT);
        map.put(SzBadInputException.class, Status.INVALID_ARGUMENT);
        map.put(IllegalStateException.class, Status.FAILED_PRECONDITION);
        map.put(SzConfigurationException.class, Status.FAILED_PRECONDITION);
        map.put(SzReplaceConflictException.class, Status.FAILED_PRECONDITION);
        map.put(SzNotInitializedException.class, Status.FAILED_PRECONDITION);
        map.put(SzRetryTimeoutExceededException.class, Status.DEADLINE_EXCEEDED);
        map.put(SzLicenseException.class, Status.RESOURCE_EXHAUSTED);
        map.put(SzRetryableException.class, Status.OUT_OF_RANGE);
        map.put(SzException.class, Status.INTERNAL);

        STATUS_MAP = Collections.unmodifiableMap(map);
    }

    /**
     * The number of milliseconds to provide advance warning of an expiring
     * license.
     */
    private static final long EXPIRATION_WARNING_MILLIS = Duration.ofDays(30).toMillis();

    /**
     * The {@link DateTimeFormatter} to use for parsing the license
     * expiration date.
     */
    private static final DateTimeFormatter DATE_FORMAT
        = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.UK);

    /**
     * The proxied {@link SzEnvironment} to prevent calling of
     * {@link SzEnvironment#destroy()}.
     */
    private SzEnvironment proxyEnvironment = null;

    /**
     * The built {@link GrpcService} containing all Senzing service implementations.
     */
    private GrpcService grpcService = null;

    /**
     * The {@link SzReplicator} if the data mart has been configured.
     */
    private SzReplicator replicator = null;

    /**
     * The {@link ObjectMapper} for converting the response objects
     * for HTTP to JSON text.
     */
    private ObjectMapper objectMapper = null;

    /**
     * Tracks if {@link #configureServer(ServerBuilder)} has been called.
     */
    private boolean configured = false;

    /**
     * Tracks if this instance has been started.
     */
    private boolean started = false;

    /**
     * Tracks if this instance has been destroyed.
     */
    private boolean destroyed = false;

    /**
     * Constructs with the specified {@link SzEnvironment} and no
     * data mart replication.
     *
     * @param env The {@link SzEnvironment} to use.
     */
    public SzGrpcServices(SzEnvironment env) {
        this(env, null, null);
    }

    /**
     * Constructs with the specified {@link SzEnvironment} and optional
     * data mart replication.
     *
     * @param env               The {@link SzEnvironment} to use.
     * @param dataMartUri       The resolved {@link ConnectionUri} for the
     *                          data mart database, or {@code null} if data
     *                          mart replication is not desired.
     * @param processingRate    The {@link ProcessingRate} for data mart
     *                          replication, or {@code null} for the default.
     */
    public SzGrpcServices(SzEnvironment    env,
                          ConnectionUri    dataMartUri,
                          ProcessingRate   processingRate)
    {
        // proxy the environment to prevent destroy() calls
        this.proxyEnvironment = (SzEnvironment) restrictedProxy(env, DESTROY_METHOD);

        // build the replicator if data mart is configured
        if (dataMartUri != null) {
            SzReplicatorOptions replicatorOptions = new SzReplicatorOptions();
            replicatorOptions.setUsingDatabaseQueue(true);
            replicatorOptions.setProcessingRate(processingRate);
            replicatorOptions.setDatabaseUri(dataMartUri);

            try {
                this.replicator = new SzReplicator(this.proxyEnvironment,
                        replicatorOptions,
                        false);

            } catch (RuntimeException e) {
                logError(e, "Failed to initialize replicator with URI: " + dataMartUri);
                throw e;

            } catch (Exception e) {
                logError(e, "Failed to initialize replicator with URI: " + dataMartUri);
                throw new RuntimeException("Failed to initialize data mart", e);
            }
        }

        // build the gRPC service with all Senzing service implementations
        this.grpcService = GrpcService.builder()
                .useBlockingTaskExecutor(true)
                .addService(new SzGrpcProductImpl(this))
                .addService(new SzGrpcConfigImpl(this))
                .addService(new SzGrpcConfigManagerImpl(this))
                .addService(new SzGrpcDiagnosticImpl(this))
                .addService(new SzGrpcEngineImpl(this))
                .build();
    }

    /**
     * Configures all Senzing services onto the provided
     * {@link ServerBuilder}. This adds the gRPC service and, if data
     * mart replication is configured, the data mart report endpoints
     * under the {@link #DATA_MART_PREFIX} path.
     *
     * <p>This method does <b>not</b> configure port, bind address,
     * concurrency, or CORS &mdash; those concerns belong to the owner of
     * the {@link ServerBuilder}.</p>
     *
     * @param builder The {@link ServerBuilder} to configure.
     */
    public synchronized void configureServer(ServerBuilder builder) {
        if (this.configured) {
            throw new IllegalStateException(
                "This instance has already configured a server");
        }
        this.configured = true;

        // add the gRPC service
        builder.service(this.grpcService);

        // add data mart report services if configured
        if (this.replicator != null) {
            SzReplicationProvider provider = this.replicator.getReplicationProvider();

            DataMartReportsServices dataMartReports = new DataMartReportsServices(
                    this.proxyEnvironment,
                    provider.getConnectionProvider());

            this.objectMapper = new ObjectMapper();

            AnnotatedServiceBindingBuilder serviceBuilder = builder.annotatedService()
                    .pathPrefix(DATA_MART_PREFIX)
                    .requestConverters(
                            new JacksonRequestConverterFunction(this.objectMapper))
                    .responseConverters(
                            new JacksonResponseConverterFunction(this.objectMapper));

            serviceBuilder.build(dataMartReports);
        }
    }

    /**
     * Gets the {@link SzEnvironment} used by this instance.
     *
     * The returned instance is a {@link java.lang.reflect.Proxy} that
     * will not allow the caller to invoke {@link SzEnvironment#destroy()}.
     *
     * @return The {@link SzEnvironment} used by this instance.
     */
    public SzEnvironment getEnvironment() {
        return this.proxyEnvironment;
    }

    /**
     * Gets the {@link SzReplicationProvider} for this instance
     * if the data mart has been enabled. This returns
     * {@code null} if the data mart is not enabled.
     *
     * @return The {@link SzReplicationProvider} for this instance,
     *         or {@code null} if data mart replication is not
     *         enabled.
     */
    public SzReplicationProvider getReplicationProvider() {
        return (this.replicator == null) ? null
                : this.replicator.getReplicationProvider();
    }

    /**
     * Gets the {@link SQLConsumer.MessageQueue} for enqueuing INFO
     * messages for consumption by the data mart. This returns
     * {@code null} if the data mart is not enabled.
     *
     * @return The {@link SQLConsumer.MessageQueue} for enqueuing INFO
     *         messages for consumption by the data mart, or
     *         {@code null} if the data mart is not enabled.
     */
    public SQLConsumer.MessageQueue getDataMartMessageQueue() {
        return (this.replicator == null) ? null
                : this.replicator.getDatabaseMessageQueue();
    }

    /**
     * Checks if this instance has been destroyed.
     *
     * @return {@code true} if this instance has been destroyed,
     *         otherwise {@code false}.
     */
    public synchronized boolean isDestroyed() {
        return this.destroyed;
    }

    /**
     * Starts the data mart replicator (if configured) and the license
     * expiration monitoring thread.  This should be called after the
     * Armeria server has been started.
     *
     * @throws IllegalStateException If this instance has already been
     *                               destroyed.
     */
    public synchronized void start() {
        if (this.destroyed) {
            throw new IllegalStateException(
                    "This instance has already been destroyed");
        }
        if (this.started) {
            return;
        }
        this.started = true;
        if (this.replicator != null) {
            this.replicator.start();
        }

        // start license expiration monitoring thread
        Thread monitorThread = new Thread(() -> {
            DateTimeFormatter formatter = DateTimeFormatter
                .ofLocalizedDate(FormatStyle.LONG)
                .withZone(ZoneId.systemDefault());

            while (!this.isDestroyed()) {
                try {
                    SzProduct product = this.getEnvironment().getProduct();
                    String license = product.getLicense();
                    JsonObject jsonObject
                        = JsonUtilities.parseJsonObject(license);
                    String dateText = JsonUtilities.getString(
                        jsonObject, "expireDate");
                    LocalDateTime expireDateTime = (dateText == null)
                        ? null : LocalDateTime.parse(dateText, DATE_FORMAT);

                    if (expireDateTime != null) {
                        Instant expiration
                            = expireDateTime.toInstant(ZoneOffset.UTC);
                        long diff = expiration.toEpochMilli()
                            - System.currentTimeMillis();

                        if (diff < 0L) {
                            logWarning(
                                "WARNING: License expired -- was valid "
                                + "through "
                                + formatter.format(expiration) + ".");

                        } else if (diff < EXPIRATION_WARNING_MILLIS) {
                            logWarning(
                                "WARNING: License expiring soon -- valid "
                                + "through "
                                + formatter.format(expiration) + ".");
                        }
                    }
                    synchronized (this) {
                        if (!this.isDestroyed()) {
                            this.wait(Duration.ofHours(6).toMillis());
                        }
                    }
                } catch (InterruptedException ignore) {
                    // do nothing
                } catch (Exception failure) {
                    logWarning(failure,
                        "License monitoring encountered an error; "
                        + "will retry");
                    try {
                        synchronized (this) {
                            if (!this.isDestroyed()) {
                                this.wait(
                                    Duration.ofMinutes(30).toMillis());
                            }
                        }
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                }
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    /**
     * Destroys this instance, shutting down the data mart replicator
     * (if configured) and the license expiration monitoring thread.
     */
    public synchronized void destroy() {
        if (this.destroyed) {
            return;
        }
        try {
            if (this.replicator != null) {
                this.replicator.shutdown();
            }
        } finally {
            this.destroyed = true;
            this.notifyAll();
        }
    }

    /**
     * Attempt to infer the {@link Status} from the {@link Throwable}.
     * If it cannot be inferred then {@link Status#UNKNOWN} is returned.
     *
     * @param t The {@link Throwable} from which to refer the {@link Status}.
     * @return The inferred {@link Status}.
     */
    protected static io.grpc.Status inferStatus(Throwable t) {
        if (t == null) {
            return Status.UNKNOWN;
        }
        for (Map.Entry<Class<?>, Status> entry : STATUS_MAP.entrySet()) {
            if (entry.getKey().isInstance(t)) {
                return entry.getValue();
            }
        }
        return Status.UNKNOWN;
    }

    /**
     * Creates a {@link StatusRuntimeException} from the optionally-specified
     * {@link Status} and the specified {@link Throwable}.
     *
     * @param t The {@link Throwable} instance to use as a basis.
     *
     * @return The {@link StatusRuntimeException} that was created.
     */
    protected static StatusRuntimeException toStatusRuntimeException(Throwable t) {
        return toStatusRuntimeException(inferStatus(t), t);
    }

    /**
     * Creates a {@link StatusRuntimeException} from the optionally-specified
     * {@link Status} and the specified {@link Throwable}.
     *
     * @param status The explicit {@link Status} or <code>null</code> if the
     *               {@link Status} should be inferred from the {@link Throwable}.
     *
     * @param t      The {@link Throwable} instance to use as a basis.
     *
     * @return The {@link StatusRuntimeException} that was created.
     */
    protected static StatusRuntimeException toStatusRuntimeException(Status status,
            Throwable t) {
        // check if the throwable is null
        if (t == null) {
            status = (status == null) ? Status.UNKNOWN : status;
            return status.asRuntimeException();
        }

        // check if status is null and infer it if so
        if (status == null) {
            status = inferStatus(t);
        }

        // setup the fields for the JSON error
        String reason = null;

        // check if we have an SzException
        if (t instanceof SzException) {
            SzException sze = (SzException) t;

            String prefix = REASON_PREFIX + sze.getErrorCode()
                    + REASON_SPLITTER;
            String message = sze.getMessage();

            if (message.startsWith(prefix)) {
                reason = message;
            } else {
                reason = prefix + message;
            }
        }

        // get the other fields
        StackTraceElement[] stackFrames = t.getStackTrace();

        StackTraceElement relevantFrame = null;
        for (StackTraceElement ste : stackFrames) {
            if (ste.getClassName().startsWith("com.senzing")) {
                String className = ste.getClassName();
                String methodName = ste.getMethodName();
                if (methodName == null) {
                    continue;
                }
                // skip the utility method for creating exceptions
                if (className.equals(SzCoreUtilities.class.getName())
                        && methodName.equals("createSzException")) {
                    continue;
                }
                // skip the core env method for handling return codes
                if (className.equals(SzCoreEnvironment.class.getName())
                        && methodName.equals("handleReturnCode")) {
                    continue;
                }

                // any other senzing method, assume it is relevant
                relevantFrame = ste;
                break;
            }
        }

        // format the relevant frame
        String function = (relevantFrame == null)
                ? null
                : formatStackFrame(relevantFrame);

        // get the stack trace list
        List<String> stackTrace = new ArrayList<>(stackFrames.length);
        for (StackTraceElement frame : stackFrames) {
            stackTrace.add(formatStackFrame(frame));
        }

        // get the text for the original exception
        String text = t.toString();

        // set the fields
        JsonObjectBuilder job = Json.createObjectBuilder();
        if (reason != null) {
            job.add(REASON_FIELD_KEY, reason);
        }
        if (text != null) {
            job.add(TEXT_FIELD_KEY, text);
        }
        if (function != null) {
            job.add(FUNCTION_FIELD_KEY, function);
        }
        if (stackTrace != null) {
            JsonArrayBuilder jab = Json.createArrayBuilder();
            for (String frame : stackTrace) {
                jab.add(frame);
            }
            job.add(STACK_TRACE_FIELD_KEY, jab);
        }
        JsonObjectBuilder wrapper = Json.createObjectBuilder();
        wrapper.add(ERROR_FIELD_KEY, job);

        String jsonError = toJsonText(wrapper);

        // create the exception
        return status.withDescription(jsonError)
                .withCause(t)
                .asRuntimeException();
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
     */
    public static String getString(GeneratedMessage message, String fieldName) {
        Descriptor descriptor = message.getDescriptorForType();
        FieldDescriptor fieldDesc = descriptor.findFieldByName(fieldName);
        if (fieldDesc == null) {
            throw new IllegalArgumentException(
                    "Field (" + fieldName + ") not recognized for type: "
                            + message.getClass().getName());
        }
        if (!message.hasField(fieldDesc)) {
            return null;
        }
        Object result = message.getField(fieldDesc);
        return (result == null) ? null : result.toString();
    }
}

package com.senzing.sdk.grpc;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzProduct;
import com.senzing.sdk.core.SzCoreUtilities;

import static com.senzing.sdk.grpc.proto.SzEngineProto.*;
import static com.senzing.util.JsonUtilities.*;

/**
 * Provides a gRPC implementation of {@link SzEnvironment}.
 */
public class SzGrpcEnvironment implements SzEnvironment {
    /**
     * The "error" JSON property key for the gRPC error messages.
     */
    public static final String ERROR_FIELD_KEY = "error";

    /**
     * The "reason" JSON property key for the gRPC error messages.
     */
    public static final String REASON_FIELD_KEY = "reason";

    /**
     * The "function" JSON property key for the gRPC error messages.
     */
    public static final String FUNCTION_FIELD_KEY = "function";

    /**
     * The "text" JSON property key for the gRPC error messages.
     */
    public static final String TEXT_FIELD_KEY = "text";

    /**
     * The "stackTrace" JSON property key for the gRPC error messages.
     */
    public static final String STACK_TRACE_FIELD_KEY = "stackTrace";

    /**
     * The prefix for the reason string in a Senzing gRPC error.
q     */
    public static final String REASON_PREFIX = "SENZ";

    /**
     * The string that splits the error code from the error message
     * in the gRPC error reason.
     */
    public static final String REASON_SPLITTER = "|";

    /**
     * Enumerates the possible states for an instance of {@link SzGrpcEnvironment}.
     */
    private enum State {
        /**
         * If an {@link SzGrpcEnvironment} instance is in the "active" state then it
         * is initialized and ready to use.  Only one instance of {@link 
         * SzGrpcEnvironment} can exist in the {@link #ACTIVE} or {@link #DESTROYING}
         * state because Senzing environment cannot be initialized heterogeneously
         * within a single process.
         * 
         * @see SenzingSdk#getActiveInstance()
         */
        ACTIVE,

        /**
         * An instance {@link SzGrpcEnvironment} moves to the "destroying" state when
         * the {@link #destroy()} method has been called but has not yet completed any
         * Senzing operations that are already in-progress.  In this state, the
         * {@link SzGrpcEnvironment} will fast-fail any newly attempted operations with
         * an {@link IllegalStateException}, but will complete those Senzing operations
         * that were initiated before {@link #destroy()} was called.
         */
        DESTROYING,

        /**
         * An instance of {@link SzGrpcEnvironment} moves to the "destroyed" state when
         * the {@link #destroy()} method has completed and there are no more Senzing
         * operations that are in-progress.  Once an {@link #ACTIVE} instance moves to
         * {@link #DESTROYED} then a new active instance can be created and initialized.
         */
        DESTROYED;
    }

    /**
     * The {@link SzGrpcProduct} singleton instance to use.
     */
    private SzGrpcProduct grpcProduct = null;

    /**
     * The {@link SzGrpcEngine} singleton instance to use.
     */
    private SzGrpcEngine grpcEngine = null;

    /**
     * The {@link SzGrpcConfigManager} singleton instance to use.
     */
    private SzGrpcConfigManager grpcConfigMgr = null;

    /**
     * The {@link SzGrpcDiagnostic} singleton instance to use.
     */
    private SzGrpcDiagnostic grpcDiagnostic = null;

    /**
     * The underlying GRPC channel to use as provided during construction.
     * <b>NOTE:</b> This is opened and managed (and closed) externally to this instance.
     */
    private Channel grpcChannel = null;

    /**
     * The {@link State} for this instance.
     */
    private State state = null;

    /** 
     * The number of currently executing operations.
     */
    private int executingCount = 0;

    /**
     * The {@link ReadWriteLock} for this instance.
     */
    private final ReadWriteLock readWriteLock;

    /**
     * Internal object for instance-wide synchronized locking.
     */
    private final Object monitor = new Object();

    /**
     * Provides an interface for initializing an instance of
     * {@link SzGrpcEnvironment}.
     * 
     * <p>
     * This interface is not needed to use {@link SzGrpcEnvironment}.
     * It is only needed if you are extended {@link SzGrpcEnvironment}.
     * </p>
     * 
     * <p>
     * This is provided for derived classes of {@link SzGrpcEnvironment}
     * to initialize their super class and is typically implemented by
     * extending {@link AbstractBuilder} in creating a derived
     * builder implementation.
     * </p>
     */
    public interface Initializer {
        /**
         * Gets the gRPC {@link Channel} to use.
         * 
         * @return The gRPC {@link Channel} to use.
         */
        Channel getChannel();
    }

    /**
     * Provides a base class for builder implementations of
     * {@link SzGrpcEnvironment} and its derived classes.
     * 
     * <p>
     * This class is not used in the usage of {@link SzGrpcEnvironment}.
     * It is only needed if you are extending {@link SzGrpcEnvironment}.
     * </p>
     * 
     * <p>
     * This class allows the derived builder to return references to its
     * own environment class and to its own builder type rather than base 
     * classes.  When extending {@link SzGrpcEnvironment} you should
     * provide an implementation of this class that is specific to your
     * derived class.
     * </p>
     * 
     * @param <E> The {@link SzGrpcEnvironment}-derived class built by instances
     *            of this class.
     * @param <B> The {@link AbstractBuilder}-derived class of the implementation.
     */
    public abstract static class AbstractBuilder<
        E extends SzGrpcEnvironment, B extends AbstractBuilder<E, B>>
        implements Initializer 
    {
        /**
         * The gRPC {@link Channel} for this instance.
         */
        private Channel channel = null;

        /**
         * Default constructor.
         */
        protected AbstractBuilder() {
            this.channel = null;
        }


        /**
         * Gets the gRPC {@link Channel} with which to initialize the 
         * {@link SzGrpcEnvironment}.
         * 
         * @return The gRPC {@link Channel} with which to initialize the 
         *         {@link SzGrpcEnvironment}.
         * 
         */
        @Override
        public Channel getChannel() {
            return this.channel;
        }

        /**
         * Provides the gRPC {@link Channel} to initialize the {@link SzGrpcEnvironment}.
         * 
         * @param channel The gRPC {@link Channel} to initialize the
         *                {@link SzGrpcEnvironment}.
         * 
         * @return A reference to this instance.
         */
        @SuppressWarnings("unchecked")
        public B channel(Channel channel) {
            Objects.requireNonNull(channel, "The gRPC channel cannot be null");
            this.channel = channel;
            return ((B) this);
        }

        /**
         * Implement this method to create a new {@link SzGrpcEnvironment}
         * instance of type <code>E</code> based on this builder instance.
         * 
         * @return The newly created {@link SzGrpcEnvironment} instance
         *         of type <code>E</code>.
         */
        public abstract E build() throws IllegalStateException;
    }

    /**
     * The builder class for creating an instance of {@link SzGrpcEnvironment}.
     */
    public static class Builder extends AbstractBuilder<SzGrpcEnvironment, Builder>
    {
        /**
         * Default constructor.
         */
        public Builder() {
            super();
        }

        /**
         * Creates a new {@link SzGrpcEnvironment} instance based on this
         * {@link Builder} instance.
         * 
         * @return The newly created {@link SzGrpcEnvironment} instance.
         */
        @Override
        public SzGrpcEnvironment build()
        {
            if (this.getChannel() == null) {
                throw new IllegalStateException(
                    "The channel has not yet been provided, but is required.");
            }
            return new SzGrpcEnvironment(this);
        }
    }

    /**
     * Creates a new instance of {@link Builder} for setting up an instance
     * of {@link SzEnvironment}.
     * 
     * <p>
     * <b>Alternatively</b>, you can directly call the {@link Builder#Builder()}
     * constructor.
     * </p>
     * 
     * @return The {@link Builder} for configuring and initializing the
     *         {@link SzGrpcEnvironment}.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Constructs with the specified {@link Channel}.  The constructed
     * instance will <b>not</b> shutdown the channel even upon calling
     * {@link #destroy()}.  However, you can be assured the constructed
     * instance will <b>not</b> use the specified {@link Channel} once
     * it is {@linkplain #isDestroyed() destroyed}.
     *  
     * @param initializer The {@link Initializer} with which to construct.
     */
    protected SzGrpcEnvironment(Initializer initializer) 
    {
        Objects.requireNonNull(initializer, "The Initializer cannot be null");
        Objects.requireNonNull(
            initializer.getChannel(), 
            "The Initializer is invalid.  The gRPC Channel cannot be null");
        
        // set the fields
        this.readWriteLock  = new ReentrantReadWriteLock(true);
        this.grpcChannel    = initializer.getChannel();
        this.state          = State.ACTIVE;
    }

    /**
     * Package-private method for obtaining the underling GRPC channel.
     * 
     * @return The {@link Channel} with which this instance was constructed.
     * 
     * @throws IllegalStateException If this instance has already been destroyed.
     */
    Channel getChannel() {
        synchronized (this.monitor) {
            this.ensureActive();
            return this.grpcChannel;
        }
    }

    /**
     * Executes the specified {@link Callable} task and returns the result
     * if successful.  This will throw any exception produced by the {@link 
     * Callable} task, wrapping it in an {@link SzException} if it is a
     * checked exception that is not of type {@link SzException}.
     * 
     * <p>
     * If overriding ths method, you should throw an {@link IllegalStateException}
     * if this instance has already been {@linkplain #destroy() destroyed},
     * otherwise, this method must complete execution of the specified {@link 
     * Callable} task even if {@link #destroy()} is called after this method
     * is called but before it completes.  Further, {@link #destroy()} should
     * not complete until <b>all</b> in-flight tasks complete.
     * </p>
     * 
     * @param <T> The return type.
     * @param task The {@link Callable} task to execute.
     * @return The result from the {@link Callable} task.
     * @throws SzException If the {@link Callable} task triggers a failure.
     * @throws IllegalStateException If this {@link SzGrpcEnvironment} instance has
     *                               already been destroyed.
     */
    protected <T> T execute(Callable<T> task)
        throws SzException, IllegalStateException
    {
        Lock lock = null;
        try {
            // acquire a write lock while checking if active
            lock = this.acquireReadLock();
            synchronized (this.monitor) {
                if (this.state != State.ACTIVE) {
                    throw new IllegalStateException(
                        "SzEnvironment has been destroyed");
                }

                // increment the executing count
                this.executingCount++;
            }
        
            return task.call();

        } catch (StatusRuntimeException e) {
            throw createSzException(e.getStatus(), e);

        } catch (SzException | RuntimeException e) {
            throw e;

        } catch (Exception e) {
            throw new SzException(e);

        } finally {
            synchronized (this.monitor) {
                this.executingCount--;
                this.monitor.notifyAll();
            }
            lock = releaseLock(lock);
        }
    }

    /**
     * Creates an {@link SzException} from the specified {@link Status}
     * and {@link Exception}.
     * 
     * @param status The gRPC {@link Status} describing the failure.
     * 
     * @param e The original {@link Exception} describing the cause 
     *          of the failure (typically an instance of 
     *          {@link io.grpc.StatusException} or {@link StatusRuntimeException}).
     * 
     * @return A new {@link SzException} representing the specified
     *         {@link StatusRuntimeException}.
     */
    public static SzException createSzException(Status status, Exception e) 
    {   
        String description = status.getDescription();
        if (description != null) {
            description = description.trim();
        }

        // no description, handle that
        if (description == null) {
            return new SzException(e);
        }

        try {
            // check if we do not have JSON for some reason
            if (!description.startsWith("{") || !description.endsWith("}")) {
                return new SzException(description, e);
            }

            // try to parse as a JsonObject
            JsonObject jsonObj = null;
            try {
                jsonObj = Json.createReader(
                    new StringReader(description)).readObject();
            } catch (Exception e2) {
                return new SzException(description, e);
            }

            // find the senzing reason
            String reason = null;
            while (reason == null && jsonObj != null) {
                // check for a reason
                reason = getString(jsonObj, REASON_FIELD_KEY);

                // check if the reason was not found
                if (reason == null) {
                    // get the next-level "error" sub-object
                    jsonObj = getJsonObject(jsonObj, ERROR_FIELD_KEY);
                }
            }

            // check if no reason
            if (reason == null) {
                return new SzException(description, e);
            }

            // check if the reason begins with expected prefix
            if (!reason.startsWith(REASON_PREFIX)) {
                return new SzException(description, e);
            }

            // check for the index of the "|" character
            int index = reason.indexOf(REASON_SPLITTER);
            if (index < (REASON_PREFIX.length() + 1)) {
                return new SzException(description, e);
            }

            // get the error code text
            String codeText = reason.substring(REASON_PREFIX.length(), index);
            int errorCode = 0;
            try {
                errorCode = Integer.parseInt(codeText);
            } catch (Exception e2) {
                // did not parse as an integer
                return new SzException(description, e);
            }
            
            // get the error message
            String message = (index < (reason.length() - 1)) 
                ? reason.substring(index + 1) : "";

            // attempt to get additional information for the exception
            String          text        = getString(jsonObj, TEXT_FIELD_KEY);
            String          function    = getString(jsonObj, FUNCTION_FIELD_KEY);
            List<String>    stackTrace  = getStrings(jsonObj, STACK_TRACE_FIELD_KEY);

            if (text != null || function != null || stackTrace != null) {
                StringWriter    sw = new StringWriter();
                PrintWriter     pw = new PrintWriter(sw);
                pw.println(message);
                if (text != null && text.trim().length() > 0) {
                    pw.println();
                    pw.println("Original Server Text: " + text);
                }
                if (function != null && function.trim().length() > 0) {
                    pw.println();
                    pw.println("Server Function: " + function);
                }
                if (stackTrace != null && stackTrace.size() > 0) {
                    pw.println();
                    pw.println("Server Stack Trace: ");
                    for (String stackFrame : stackTrace) {
                        pw.println("  - " + stackFrame);
                    }
                }
                pw.flush(); // for good measure
                message = sw.toString();
            }
        
            // now return the SzException for the error code and message
            return SzCoreUtilities.createSzException(errorCode, message);

        } catch (JsonException e2) {
            // the does not appear to be JSON, use it directly
            return new SzException(description, e);
        }
    }

    /**
     * Gets the number of currently executing operations.
     * 
     * @return The number of currently executing operations.
     */
    int getExecutingCount() {
        synchronized (this.monitor) {
            return this.executingCount;
        }
    }

    /**
     * Ensures this instance is still active and if not will throw 
     * an {@link IllegalStateException}.
     *
     * @throws IllegalStateException If this instance is not active.
     */
    void ensureActive() throws IllegalStateException {
        synchronized (this.monitor) {
            if (this.state != State.ACTIVE) {
                throw new IllegalStateException(
                    "The SzGrpcEnvironment instance has already been destroyed.");
            }
        }
    }

    /**
     * Returns an {@link SzProduct} implementation that will execute its 
     * operations over the gRPC protocol to the server associated with this
     * instance.  This implementation returns an instance of 
     * {@link SzGrpcProduct}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SzProduct getProduct() throws IllegalStateException, SzException {
        synchronized (this.monitor) {
            this.ensureActive();
            if (this.grpcProduct == null) {
                this.grpcProduct = new SzGrpcProduct(this);
            }
            // return the configured instance
            return this.grpcProduct;
        }
    }

    /**
     * Returns an {@link SzEngine} implementation that will execute its 
     * operations over the gRPC protocol to the server associated with this
     * instance.  This implementation returns an instance of 
     * {@link SzGrpcEngine}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SzEngine getEngine() throws IllegalStateException, SzException {
        synchronized (this.monitor) {
            this.ensureActive();
            if (this.grpcEngine == null) {
                this.grpcEngine = new SzGrpcEngine(this);
            }
            // return the configured instance
            return this.grpcEngine;
        }
    }

    /**
     * Returns an {@link SzConfigManager} implementation that will execute its 
     * operations over the gRPC protocol to the server associated with this
     * instance.  This implementation returns an instance of 
     * {@link SzGrpcConfigManager}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SzConfigManager getConfigManager() throws IllegalStateException, SzException {
        synchronized (this.monitor) {
            this.ensureActive();
            if (this.grpcConfigMgr == null) {
                this.grpcConfigMgr = new SzGrpcConfigManager(this);
            }

            // return the configured instance
            return this.grpcConfigMgr;
        }
    }

    /**
     * Returns an {@link SzDiagnostic} implementation that will execute its 
     * operations over the gRPC protocol to the server associated with this
     * instance.  This implementation returns an instance of 
     * {@link SzGrpcDiagnostic}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SzDiagnostic getDiagnostic() throws IllegalStateException, SzException {
        synchronized (this.monitor) {
            this.ensureActive();
            if (this.grpcDiagnostic == null) {
                this.grpcDiagnostic = new SzGrpcDiagnostic(this);
            }
            // return the configured instance
            return this.grpcDiagnostic;
        }
    }

    /**
     * Implemented to execute the operation over the gRPC protocol against
     * the associated gRPC server and return the active configuration ID 
     * from the gRPC server's {@link SzEnvironment}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public long getActiveConfigId() throws IllegalStateException, SzException {
        Lock lock = null;
        try {
            // get a read lock to ensure we remain active while
            // executing the operation
            lock = this.acquireReadLock();
            
            // ensure we have initialized the engine or diagnostic
            synchronized (this.monitor) {
                this.ensureActive();

                // check if the core engine has been initialized
                if (this.grpcEngine == null) {
                    // initialize the engine if not yet initialized
                    this.getEngine();
                }
            }

            // get the active config ID from the gRPC server
            GetActiveConfigIdRequest request = GetActiveConfigIdRequest.newBuilder().build();

            // get the response
            GetActiveConfigIdResponse response = this.execute(() -> {
                return this.grpcEngine.getBlockingStub().getActiveConfigId(request);
            });
            
            // return the config ID
            return response.getResult();
            
        } finally {
            lock = this.releaseLock(lock);
        }
    }

    /**
     * Implemented to execute the operation over the gRPC protocol against
     * the associated gRPC server and reinitialize gRPC server's
     * {@link SzEnvironment} (assuming the operation is allowed by the server).
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void reinitialize(long configId) throws IllegalStateException, SzException {
        Lock lock = null;
        try {
            // get a read lock to ensure we remain active while
            // executing the operation
            lock = this.acquireReadLock();
            
            // ensure we have initialized the engine or diagnostic
            synchronized (this.monitor) {
                this.ensureActive();

                // check if the core engine has been initialized
                if (this.grpcEngine == null) {
                    // initialize the engine if not yet initialized
                    this.getEngine();
                }
            }

            // reinitialize the gRPC server
            ReinitializeRequest request = ReinitializeRequest
                .newBuilder().setConfigId(configId).build();

            // execute the reinitialization
            this.execute(() -> {
                return this.grpcEngine.getBlockingStub().reinitialize(request);
            });
            
        } finally {
            lock = this.releaseLock(lock);
        }
    }

    /**
     * Implemented to destroy this instance with <b>no</b> effect on the 
     * the associated gRPC server's {@link SzEnvironment}.  This will block
     * until all in-flight operations from {@link #execute(Callable)} 
     * complete, but will prevent further tasks to be invoked via the 
     * {@link #execute(Callable)} method.
     * 
     * <p>
     * <b>NOTE:</b> This method will <b>not</b> {@linkplain 
     * io.grpc.ManagedChannel#shutdown() shutdown} the associated gRPC
     * {@link io.grpc.ManagedChannel}.
     * 
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        Lock lock = null;
        try {
            synchronized (this.monitor) {
                // check if this has already been called
                if (this.state != State.ACTIVE) {
                    return;
                }

                // set the flag for destroying
                this.state = State.DESTROYING;
                this.monitor.notifyAll();
            }

            // acquire an exclusive lock for destroying to ensure
            // all executing tasks have completed
            lock = this.acquireWriteLock();

            // ensure completion of in-flight executions
            int exeCount = this.getExecutingCount();
            if (exeCount > 0) {
                throw new IllegalStateException(
                    "Acquired write lock for destroying environment while tasks "
                    + "still executing: " + exeCount);
            }
            
            // once we get here we can really shut things down
            this.grpcEngine = null;
            this.grpcDiagnostic = null;
            this.grpcConfigMgr = null;
            this.grpcProduct = null;
            this.grpcChannel = null;

            // set the state
            synchronized (this.monitor) {
                this.state = State.DESTROYED;
                this.monitor.notifyAll();
            }
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDestroyed() {
        synchronized (this.monitor) {
            return this.state != State.ACTIVE;
        }
    }
    
    /**
     * Acquires an exclusive write lock from this instance's
     * {@link ReentrantReadWriteLock}.
     * 
     * @return The {@link Lock} that was acquired.
     */
    private Lock acquireWriteLock() {
        Lock lock = this.readWriteLock.writeLock();
        lock.lock();
        return lock;
    }

    /**
     * Acquires a shared read lock from this instance's 
     * {@link ReentrantReadWriteLock}.
     * 
     * @return The {@link Lock} that was acquired.
     */
    private Lock acquireReadLock() {
        Lock lock = this.readWriteLock.readLock();
        lock.lock();
        return lock;
    }

    /**
     * Releases the specified {@link Lock} if not <code>null</code>.
     * 
     * @param lock The {@link Lock} to be released.
     * 
     * @return Always returns <code>null</code>.
     */
    private Lock releaseLock(Lock lock) {
        if (lock != null) {
            lock.unlock();
        }
        return null;
    }
}

package com.senzing.sdk.grpc;

import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzProduct;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.grpc.SzEngineGrpc.SzEngineBlockingStub;
import com.senzing.sdk.grpc.SzEngineProto.GetActiveConfigIdRequest;
import com.senzing.sdk.grpc.SzEngineProto.GetActiveConfigIdResponse;
import com.senzing.sdk.grpc.SzEngineProto.ReinitializeRequest;

/**
 * Provides a gRPC implementation of {@link SzEnvironment}.
 */
public class SzGrpcEnvironment implements SzEnvironment {
    /**
     * The "error" JSON property key for the gRPC error messages.
     */
    private static final String ERROR_FIELD_KEY = "error";

    /**
     * The "reason" JSON property key for the gRPC error messages.
     */
    private static final String REASON_FIELD_KEY = "reason";

    /**
     * The prefix for the reason string in a Senzing gRPC error.
     */
    private static final String REASON_PREFIX = "SENZ";

    /**
     * The string that splits the error code from the error message
     * in the gRPC error reason.
     */
    private static final String REASON_SPLITTER = "|";

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
     * Constructs with the specified {@link Channel}.  The constructed
     * instance will <b>not</b> shutdown the channel even upon calling
     * {@link #destroy()}.  However, you can be assured the constructed
     * instance will <b>not</b> use the specified {@link Channel} once
     * it is {@linkplain #isDestroyed() destroyed}.
     *  
     * @param channel The gRPC {@link Channel} to use.
     */
    private SzGrpcEnvironment(Channel channel) 
    {
        // set the fields
        this.readWriteLock  = new ReentrantReadWriteLock(true);
        this.grpcChannel    = channel;
    }

    /**
     * Package-private method for obtaining the underling GRPC channel.
     * 
     * @return The {@link Channel} with which this instance was constructed.
     * 
     * @throws IllegalStateException If this instance has already been destroyed.
     */
    Channel getChannel() {
        this.ensureActive();
        return this.grpcChannel;
    }

    /**
     * Executes the specified {@link Callable} task and returns the result
     * if successful.  This will throw any exception produced by the {@link 
     * Callable} task, wrapping it in an {@link SzException} if it is a
     * checked exception that is not of type {@link SzException}.
     * 
     * @param <T> The return type.
     * @param task The {@link Callable} task to execute.
     * @return The result from the {@link Callable} task.
     * @throws SzException If the {@link Callable} task triggers a failure.
     * @throws IllegalStateException If this {@link SzGrpcEnvironment} instance has
     *                               already been destroyed.
     */
    <T> T execute(Callable<T> task)
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

        try {
            // try to parse as a JsonObject
            JsonObject jsonObj = Json.createReader(
                new StringReader(description)).readObject();
            
            // get the top-level "error" sub-object
            jsonObj = jsonObj.getJsonObject(ERROR_FIELD_KEY);
            if (jsonObj == null) {
                // if not present then return a default SzException
                return new SzException(description, e);
            } 

            // get the second-level "error" sub-object
            jsonObj = jsonObj.getJsonObject(ERROR_FIELD_KEY);
            if (jsonObj == null) {
                // if not present then return a default SzException
                return new SzException(description, e);
            }

            // get the encoded "reason" field
            String reason = jsonObj.getString(REASON_FIELD_KEY, null);
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

            // now return the SzException for the error code and message
            return SzCoreEnvironment.createSzException(errorCode, message);

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

    @Override
    public void reinitialize(long configId) throws IllegalStateException, SzException {
        throw new UnsupportedOperationException(
            "Cannot reinitialize gRPC server from gRPC client");
    }

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

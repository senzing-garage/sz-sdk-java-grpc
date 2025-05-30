package com.senzing.sdk.grpc;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.grpc.Channel;

import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzProduct;

/**
 * Provides a gRPC implementation of {@link SzEnvironment}.
 */
public class SzGrpcEnvironment implements SzEnvironment {

    /**
     * The number of milliseconds to delay (if not notified) until checking
     * if we are ready to destroy.
     */
    private static final long DESTROY_DELAY = 5000L;

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

    }

    /**
     * Waits until the specified {@link SzGrpcEnvironment} instance has been destroyed.
     * Use this when obtaining an instance of {@link SzGrpcEnvironment} in the {@link 
     * State#DESTROYING} and you want to wait until it is fully destroyed.
     * 
     * @param environment The non-null {@link SzGrpcEnvironment} instance to wait on.
     * 
     * @throws NullPointerException If the specified parameter is <code>null</code>.
     */
    private static void waitUntilDestroyed(SzGrpcEnvironment environment) 
    {
        Objects.requireNonNull(environment, "The specified instance cannot be null");
        synchronized (environment.monitor) {
            while (environment.state != State.DESTROYED) {
                try {
                    environment.monitor.wait(DESTROY_DELAY);
                } catch (InterruptedException ignore) {
                    // ignore the exception
                }
            }
        }
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getProduct'");
    }

    @Override
    public SzEngine getEngine() throws IllegalStateException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEngine'");
    }

    @Override
    public SzConfigManager getConfigManager() throws IllegalStateException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfigManager'");
    }

    @Override
    public SzDiagnostic getDiagnostic() throws IllegalStateException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDiagnostic'");
    }

    @Override
    public long getActiveConfigId() throws IllegalStateException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getActiveConfigId'");
    }

    @Override
    public void reinitialize(long configId) throws IllegalStateException, SzException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'reinitialize'");
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'destroy'");
    }

    @Override
    public boolean isDestroyed() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isDestroyed'");
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

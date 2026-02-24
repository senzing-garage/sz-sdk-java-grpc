package com.senzing.sdk.grpc.server;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import com.senzing.sdk.SzBadInputException;
import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzConfigurationException;
import com.senzing.sdk.SzDiagnostic;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzLicenseException;
import com.senzing.sdk.SzNotFoundException;
import com.senzing.sdk.SzNotInitializedException;
import com.senzing.sdk.SzProduct;
import com.senzing.sdk.SzReplaceConflictException;
import com.senzing.sdk.SzRetryTimeoutExceededException;
import com.senzing.sdk.SzRetryableException;
import com.senzing.sdk.SzUnknownDataSourceException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import static com.senzing.sdk.grpc.SzGrpcEnvironment.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SzGrpcServices}.
 *
 * <p>These tests exercise the static utility methods and lifecycle
 * behavior of {@link SzGrpcServices} without requiring a running
 * Senzing installation.  A minimal {@link SzEnvironment} proxy is
 * used for lifecycle tests.</p>
 */
@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(OrderAnnotation.class)
public class SzGrpcServicesTest {

    /**
     * Creates a minimal {@link SzEnvironment} proxy that returns
     * stub implementations for all SDK interfaces.  No real Senzing
     * installation is needed.
     *
     * @return A proxy {@link SzEnvironment}.
     */
    private static SzEnvironment createStubEnvironment() {
        InvocationHandler noOpHandler = (proxy, method, args) -> {
            Class<?> returnType = method.getReturnType();
            if (returnType == boolean.class) return false;
            if (returnType == long.class)    return 0L;
            if (returnType == int.class)     return 0;
            return null;
        };

        ClassLoader cl = SzGrpcServicesTest.class.getClassLoader();

        SzProduct product = (SzProduct) Proxy.newProxyInstance(
            cl, new Class<?>[]{ SzProduct.class }, noOpHandler);

        SzEngine engine = (SzEngine) Proxy.newProxyInstance(
            cl, new Class<?>[]{ SzEngine.class }, noOpHandler);

        SzConfig config = (SzConfig) Proxy.newProxyInstance(
            cl, new Class<?>[]{ SzConfig.class }, noOpHandler);

        SzConfigManager configMgr = (SzConfigManager) Proxy.newProxyInstance(
            cl, new Class<?>[]{ SzConfigManager.class }, noOpHandler);

        SzDiagnostic diagnostic = (SzDiagnostic) Proxy.newProxyInstance(
            cl, new Class<?>[]{ SzDiagnostic.class }, noOpHandler);

        return (SzEnvironment) Proxy.newProxyInstance(
            cl,
            new Class<?>[]{ SzEnvironment.class },
            (proxy, method, args) -> {
                String name = method.getName();
                switch (name) {
                    case "getProduct":       return product;
                    case "getEngine":        return engine;
                    case "getConfig":        return config;
                    case "getConfigManager": return configMgr;
                    case "getDiagnostic":    return diagnostic;
                    case "isDestroyed":      return false;
                    case "destroy":          return null;
                    default:
                        Class<?> rt = method.getReturnType();
                        if (rt == boolean.class) return false;
                        if (rt == long.class)    return 0L;
                        if (rt == int.class)     return 0;
                        return null;
                }
            });
    }

    // ---------------------------------------------------------------
    // inferStatus() tests
    // ---------------------------------------------------------------

    @Test
    @Order(10)
    public void testInferStatusNull() {
        assertEquals(Status.UNKNOWN.getCode(),
                     SzGrpcServices.inferStatus(null).getCode(),
                     "null throwable should map to UNKNOWN");
    }

    @Test
    @Order(11)
    public void testInferStatusUnsupportedOperation() {
        assertEquals(Status.UNIMPLEMENTED.getCode(),
                     SzGrpcServices.inferStatus(
                         new UnsupportedOperationException()).getCode(),
                     "UnsupportedOperationException should map to "
                     + "UNIMPLEMENTED");
    }

    @Test
    @Order(12)
    public void testInferStatusNullPointer() {
        assertEquals(Status.INTERNAL.getCode(),
                     SzGrpcServices.inferStatus(
                         new NullPointerException()).getCode(),
                     "NullPointerException should map to INTERNAL");
    }

    @Test
    @Order(13)
    public void testInferStatusSzNotFoundException() {
        assertEquals(Status.NOT_FOUND.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzNotFoundException(0, "test")).getCode(),
                     "SzNotFoundException should map to NOT_FOUND");
    }

    @Test
    @Order(14)
    public void testInferStatusSzUnknownDataSourceException() {
        assertEquals(Status.NOT_FOUND.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzUnknownDataSourceException(
                             0, "test")).getCode(),
                     "SzUnknownDataSourceException should map to "
                     + "NOT_FOUND");
    }

    @Test
    @Order(15)
    public void testInferStatusIllegalArgument() {
        assertEquals(Status.INVALID_ARGUMENT.getCode(),
                     SzGrpcServices.inferStatus(
                         new IllegalArgumentException()).getCode(),
                     "IllegalArgumentException should map to "
                     + "INVALID_ARGUMENT");
    }

    @Test
    @Order(16)
    public void testInferStatusSzBadInputException() {
        assertEquals(Status.INVALID_ARGUMENT.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzBadInputException(0, "test")).getCode(),
                     "SzBadInputException should map to "
                     + "INVALID_ARGUMENT");
    }

    @Test
    @Order(17)
    public void testInferStatusIllegalState() {
        assertEquals(Status.FAILED_PRECONDITION.getCode(),
                     SzGrpcServices.inferStatus(
                         new IllegalStateException()).getCode(),
                     "IllegalStateException should map to "
                     + "FAILED_PRECONDITION");
    }

    @Test
    @Order(18)
    public void testInferStatusSzConfigurationException() {
        assertEquals(Status.FAILED_PRECONDITION.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzConfigurationException(0, "test")).getCode(),
                     "SzConfigurationException should map to "
                     + "FAILED_PRECONDITION");
    }

    @Test
    @Order(19)
    public void testInferStatusSzReplaceConflictException() {
        assertEquals(Status.FAILED_PRECONDITION.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzReplaceConflictException(
                             0, "test")).getCode(),
                     "SzReplaceConflictException should map to "
                     + "FAILED_PRECONDITION");
    }

    @Test
    @Order(20)
    public void testInferStatusSzNotInitializedException() {
        assertEquals(Status.FAILED_PRECONDITION.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzNotInitializedException(
                             0, "test")).getCode(),
                     "SzNotInitializedException should map to "
                     + "FAILED_PRECONDITION");
    }

    @Test
    @Order(21)
    public void testInferStatusSzRetryTimeoutExceededException() {
        assertEquals(Status.DEADLINE_EXCEEDED.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzRetryTimeoutExceededException(
                             0, "test")).getCode(),
                     "SzRetryTimeoutExceededException should map to "
                     + "DEADLINE_EXCEEDED");
    }

    @Test
    @Order(22)
    public void testInferStatusSzLicenseException() {
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzLicenseException(0, "test")).getCode(),
                     "SzLicenseException should map to "
                     + "RESOURCE_EXHAUSTED");
    }

    @Test
    @Order(23)
    public void testInferStatusSzRetryableException() {
        assertEquals(Status.OUT_OF_RANGE.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzRetryableException(0, "test")).getCode(),
                     "SzRetryableException should map to OUT_OF_RANGE");
    }

    @Test
    @Order(24)
    public void testInferStatusSzExceptionGeneric() {
        assertEquals(Status.INTERNAL.getCode(),
                     SzGrpcServices.inferStatus(
                         new SzException(0, "test")).getCode(),
                     "Generic SzException should map to INTERNAL");
    }

    @Test
    @Order(25)
    public void testInferStatusUnmapped() {
        assertEquals(Status.UNKNOWN.getCode(),
                     SzGrpcServices.inferStatus(
                         new RuntimeException("unmapped")).getCode(),
                     "Unmapped RuntimeException should map to UNKNOWN");
    }

    // ---------------------------------------------------------------
    // toStatusRuntimeException() tests
    // ---------------------------------------------------------------

    @Test
    @Order(30)
    public void testToStatusRuntimeExceptionNullThrowable() {
        StatusRuntimeException sre
            = SzGrpcServices.toStatusRuntimeException(null);
        assertNotNull(sre, "Should not return null");
        assertEquals(Status.UNKNOWN.getCode(),
                     sre.getStatus().getCode(),
                     "Null throwable should yield UNKNOWN status");
    }

    @Test
    @Order(31)
    public void testToStatusRuntimeExceptionNonSzException() {
        RuntimeException cause
            = new RuntimeException("something broke");
        StatusRuntimeException sre
            = SzGrpcServices.toStatusRuntimeException(cause);

        assertNotNull(sre, "Should not return null");
        assertEquals(Status.UNKNOWN.getCode(),
                     sre.getStatus().getCode(),
                     "RuntimeException should yield UNKNOWN status");

        String desc = sre.getStatus().getDescription();
        assertNotNull(desc, "Description should not be null");

        // parse the JSON description
        JsonObject json = parseJsonDescription(desc);
        JsonObject error = json.getJsonObject(ERROR_FIELD_KEY);
        assertNotNull(error, "Error object should be present");

        // no reason for non-SzException
        assertFalse(error.containsKey(REASON_FIELD_KEY),
                    "Non-SzException should have no reason field");

        // text and stackTrace should be present
        assertTrue(error.containsKey(TEXT_FIELD_KEY),
                   "text field should be present");
        assertTrue(error.containsKey(STACK_TRACE_FIELD_KEY),
                   "stackTrace field should be present");
    }

    @Test
    @Order(32)
    public void testToStatusRuntimeExceptionSzException() {
        int errorCode = 42;
        String message = "entity not found";
        SzNotFoundException cause
            = new SzNotFoundException(errorCode, message);
        StatusRuntimeException sre
            = SzGrpcServices.toStatusRuntimeException(cause);

        assertNotNull(sre, "Should not return null");
        assertEquals(Status.NOT_FOUND.getCode(),
                     sre.getStatus().getCode(),
                     "SzNotFoundException should yield NOT_FOUND");

        String desc = sre.getStatus().getDescription();
        assertNotNull(desc, "Description should not be null");

        JsonObject json = parseJsonDescription(desc);
        JsonObject error = json.getJsonObject(ERROR_FIELD_KEY);
        assertNotNull(error, "Error object should be present");

        // verify reason field contains the error code
        assertTrue(error.containsKey(REASON_FIELD_KEY),
                   "SzException should have a reason field");
        String reason = error.getString(REASON_FIELD_KEY);
        assertTrue(reason.startsWith(REASON_PREFIX),
                   "Reason should start with SENZ prefix");
        assertTrue(reason.contains(REASON_SPLITTER),
                   "Reason should contain splitter");
    }

    @Test
    @Order(33)
    public void testToStatusRuntimeExceptionExplicitStatus() {
        StatusRuntimeException sre
            = SzGrpcServices.toStatusRuntimeException(
                Status.PERMISSION_DENIED,
                new RuntimeException("forbidden"));

        assertNotNull(sre, "Should not return null");
        assertEquals(Status.PERMISSION_DENIED.getCode(),
                     sre.getStatus().getCode(),
                     "Explicit status should be honoured");
    }

    @Test
    @Order(34)
    public void testToStatusRuntimeExceptionNullStatus() {
        SzException cause = new SzException(99, "some error");
        StatusRuntimeException sre
            = SzGrpcServices.toStatusRuntimeException(null, cause);

        assertNotNull(sre, "Should not return null");
        assertEquals(Status.INTERNAL.getCode(),
                     sre.getStatus().getCode(),
                     "Null status with SzException should infer "
                     + "INTERNAL");
    }

    // ---------------------------------------------------------------
    // Lifecycle tests
    // ---------------------------------------------------------------

    @Test
    @Order(40)
    public void testFreshInstanceNotDestroyed() {
        SzEnvironment env = createStubEnvironment();
        SzGrpcServices services = new SzGrpcServices(env);
        assertFalse(services.isDestroyed(),
                    "Fresh instance should not be destroyed");
        services.destroy();
    }

    @Test
    @Order(41)
    public void testDestroyMarksDestroyed() {
        SzEnvironment env = createStubEnvironment();
        SzGrpcServices services = new SzGrpcServices(env);
        services.destroy();
        assertTrue(services.isDestroyed(),
                   "Instance should be destroyed after destroy()");
    }

    @Test
    @Order(42)
    public void testDoubleDestroyIsIdempotent() {
        SzEnvironment env = createStubEnvironment();
        SzGrpcServices services = new SzGrpcServices(env);
        services.destroy();
        services.destroy(); // should not throw
        assertTrue(services.isDestroyed(),
                   "Should still be destroyed after double destroy");
    }

    @Test
    @Order(43)
    public void testStartAfterDestroyThrows() {
        SzEnvironment env = createStubEnvironment();
        SzGrpcServices services = new SzGrpcServices(env);
        services.destroy();
        assertThrows(IllegalStateException.class,
                     () -> services.start(),
                     "start() after destroy() should throw "
                     + "IllegalStateException");
    }

    @Test
    @Order(44)
    public void testDoubleStartIsIdempotent() {
        SzEnvironment env = createStubEnvironment();
        SzGrpcServices services = new SzGrpcServices(env);
        services.start();
        services.start(); // should not throw or create extra threads
        assertFalse(services.isDestroyed(),
                    "Should not be destroyed after double start");
        services.destroy();
    }

    @Test
    @Order(45)
    public void testGetEnvironmentReturnsProxy() {
        SzEnvironment env = createStubEnvironment();
        SzGrpcServices services = new SzGrpcServices(env);
        SzEnvironment proxy = services.getEnvironment();
        assertNotNull(proxy,
                      "getEnvironment() should return non-null");
        assertNotSame(env, proxy,
                      "Returned environment should be a proxy, "
                      + "not the original");
        services.destroy();
    }

    @Test
    @Order(46)
    public void testProxyEnvironmentPreventsDestroy() {
        SzEnvironment env = createStubEnvironment();
        SzGrpcServices services = new SzGrpcServices(env);
        SzEnvironment proxy = services.getEnvironment();
        assertThrows(UnsupportedOperationException.class,
                     () -> proxy.destroy(),
                     "Calling destroy() on proxy environment "
                     + "should throw");
        services.destroy();
    }

    @Test
    @Order(47)
    public void testNoReplicationByDefault() {
        SzEnvironment env = createStubEnvironment();
        SzGrpcServices services = new SzGrpcServices(env);
        assertNull(services.getReplicationProvider(),
                   "Replication provider should be null when no "
                   + "data mart is configured");
        assertNull(services.getDataMartMessageQueue(),
                   "Data mart message queue should be null when no "
                   + "data mart is configured");
        services.destroy();
    }

    // ---------------------------------------------------------------
    // Helper methods
    // ---------------------------------------------------------------

    /**
     * Parses the JSON description from a {@link StatusRuntimeException}.
     *
     * @param desc The description string from the gRPC status.
     * @return The parsed {@link JsonObject}.
     */
    private static JsonObject parseJsonDescription(String desc) {
        return Json.createReader(
            new java.io.StringReader(desc)).readObject();
    }
}

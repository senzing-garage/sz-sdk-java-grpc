package com.senzing.sdk.grpc.server;

import java.util.Objects;

import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzProduct;
import io.grpc.stub.StreamObserver;

import static com.senzing.sdk.grpc.proto.SzProductGrpc.*;
import static com.senzing.sdk.grpc.proto.SzProductProto.*;
import static com.senzing.sdk.grpc.server.SzGrpcServer.*;

/**
 * Provides the gRPC server-side implementation for {@link SzProduct}.
 */
public class SzGrpcProductImpl extends SzProductImplBase {
    /**
     * The {@link SzGrpcServer} to use.
     */
    SzGrpcServer server = null;

    /**
     * Constructs with the {@link SzGrpcServer}.
     * 
     * @param server The {@link SzGrpcServer}.
     */
    public SzGrpcProductImpl(SzGrpcServer server) {
        Objects.requireNonNull(server, "The server cannot be null");
        if (server.isDestroyed()) {
            throw new IllegalArgumentException(
                "The specified SzCoreEnvironment has already been destroyed");
        }
        this.server = server;
    }

    /**
     * Gets the {@link SzEnvironment} to use from the backing
     * {@link SzGrpcServer}.
     * 
     * @return The {@link SzEnvironment} for the backing server.
     */
    private SzEnvironment getEnvironment() {
        return this.server.getEnvironment();
    }

    @Override
    public void getLicense(GetLicenseRequest                    request, 
                           StreamObserver<GetLicenseResponse>   responseObserver)
    {
        try {
            SzProduct product = this.getEnvironment().getProduct();

            String result = product.getLicense();
            
            GetLicenseResponse response
                = GetLicenseResponse.newBuilder()
                    .setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

    @Override
    public void getVersion(GetVersionRequest                    request, 
                           StreamObserver<GetVersionResponse>   responseObserver) 
    {
        try {
            SzProduct product = this.getEnvironment().getProduct();

            String result = product.getVersion();
            
            GetVersionResponse response
                = GetVersionResponse.newBuilder()
                    .setResult(result).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(toStatusRuntimeException(e));
        }
    }

}

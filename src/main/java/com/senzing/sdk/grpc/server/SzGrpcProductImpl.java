package com.senzing.sdk.grpc.server;

import java.util.Objects;

import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzProduct;
import io.grpc.stub.StreamObserver;

import static com.senzing.sdk.grpc.proto.SzProductGrpc.*;
import static com.senzing.sdk.grpc.proto.SzProductProto.*;
import static com.senzing.sdk.grpc.server.SzGrpcServices.*;

/**
 * Provides the gRPC server-side implementation for {@link SzProduct}.
 */
public class SzGrpcProductImpl extends SzProductImplBase {
    /**
     * The {@link SzGrpcServices} to use.
     */
    private SzGrpcServices services = null;

    /**
     * Constructs with the {@link SzGrpcServices}.
     *
     * @param services The {@link SzGrpcServices}.
     */
    protected SzGrpcProductImpl(SzGrpcServices services) {
        Objects.requireNonNull(services, "The services cannot be null");
        if (services.isDestroyed()) {
            throw new IllegalArgumentException(
                "The specified SzGrpcServices has already been destroyed");
        }
        this.services = services;
    }

    /**
     * Gets the {@link SzEnvironment} to use from the backing
     * {@link SzGrpcServices}.
     *
     * @return The {@link SzEnvironment} for the backing services.
     */
    protected SzEnvironment getEnvironment() {
        return this.services.getEnvironment();
    }

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzProduct#getLicense()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

    /**
     * Implemented to execute the operation using the {@link SzEnvironment}
     * from the associated {@link SzGrpcServer} leveraging the 
     * {@link SzProduct#getVersion()} method.
     * 
     * @param request The gRPC request for the operation.
     * @param responseObserver The {@link StreamObserver} for the response.
     */
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

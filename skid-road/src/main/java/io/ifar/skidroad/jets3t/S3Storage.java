package io.ifar.skidroad.jets3t;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3Object;
import com.yammer.metrics.core.HealthCheck;

import java.io.File;

public interface S3Storage {
    HealthCheck healthCheck();

    void start() throws Exception;

    void stop() throws Exception;

    void put(String uri, File f) throws AmazonServiceException, AmazonClientException;

    S3Object get(String uri) throws AmazonServiceException, AmazonClientException;
}

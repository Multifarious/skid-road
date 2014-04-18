package io.ifar.skidroad.awssdk;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.codahale.metrics.health.HealthCheck;

import java.io.File;
import java.nio.file.Path;

public interface S3Storage {
    HealthCheck healthCheck();

    void start() throws Exception;

    void stop() throws Exception;

    void put(String uri, File f) throws AmazonServiceException, AmazonClientException;

    Path get(String uri) throws AmazonServiceException, AmazonClientException;
}

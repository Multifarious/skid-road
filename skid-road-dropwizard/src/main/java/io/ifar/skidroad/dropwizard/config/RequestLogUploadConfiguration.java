package io.ifar.skidroad.dropwizard.config;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.validation.ValidationMethod;
import org.apache.commons.lang.StringUtils;
import org.hibernate.validator.constraints.Range;

import javax.validation.Valid;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

public class RequestLogUploadConfiguration {

    @Pattern(regexp = "[A-Z0-9]{20}")
    @JsonProperty("access_key_id")
    private String accessKeyID;

    @Pattern(regexp = "[a-zA-Z0-9/+]{40}")
    @JsonProperty("secret_access_key")
    private String secretAccessKey;

    @JsonProperty("disable_certificate_checks")
    private boolean disableCertificateChecks = false;

    @NotNull
    @Valid
    @JsonProperty("upload_path")
    private String uploadPath;

    @Range(min = 1)
    @JsonProperty("max_concurrent_uploads")
    private Integer maxConcurrentUploads = 5; //depends on network bandwidth, not CPUs

    @JsonProperty("report_unhealthy_at_queue_depth")
    @DecimalMin(value="1")
    private int reportUnhealthyAtQueueDepth = 10;

    @Range(min = 1)
    @JsonProperty("retry_interval_seconds")
    private int retryIntervalSeconds= 300;

    @ValidationMethod
    public boolean isCerficateCheckingDisabled() {
        if (disableCertificateChecks) {
            System.setProperty("com.amazonaws.sdk.disableCertChecking","true");
        }
        return true;
    }

    @ValidationMethod(message = "both or neither AWS access parameter must be set.")
    public boolean isBothOrNeitherAwsAccessParameterSet() {
        return (StringUtils.isNotBlank(accessKeyID) && StringUtils.isNotBlank(secretAccessKey)) ||
                (StringUtils.isBlank(accessKeyID) && StringUtils.isBlank(secretAccessKey));
    }

    public AWSCredentialsProvider getAWSCredentialsProvider() {
        if (StringUtils.isNotBlank(accessKeyID)) {
            return new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return new BasicAWSCredentials(accessKeyID,secretAccessKey);
                }

                @Override
                public void refresh() {
                    // no op
                }
            };
        }
        return new DefaultAWSCredentialsProviderChain();
    }

    String getAccessKeyID() {
        return accessKeyID;
    }

    String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getUploadPath() {
        return uploadPath;
    }

    public Integer getMaxConcurrentUploads() {
        return maxConcurrentUploads;
    }

    public int getReportUnhealthyAtQueueDepth() {
        return reportUnhealthyAtQueueDepth;
    }

    public int getRetryIntervalSeconds() {
        return retryIntervalSeconds;
    }
}

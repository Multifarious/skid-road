package io.ifar.skidroad.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.validation.ValidationMethod;
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

    @JsonProperty("use_instance_profile_credentials")
    private boolean useInstanceProfileCredentials = false;

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

    @ValidationMethod(message = "Exactly one of access_key_id/secret_access_key and use_instance_profile_credentials" +
            " must be specified.")
    public boolean isExactlyOneAwsCredentialSpecified() {
        boolean keys = (accessKeyID != null && secretAccessKey != null);
        return (keys && !useInstanceProfileCredentials) || (useInstanceProfileCredentials && !keys);
    }

    public String getAccessKeyID() {
        return accessKeyID;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public boolean isUseInstanceProfileCredentials() {
        return useInstanceProfileCredentials;
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

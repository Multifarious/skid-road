package io.ifar.skidroad.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.Range;

import javax.validation.Valid;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

public class RequestLogUploadConfiguration {
    @NotNull
    @Pattern(regexp = "[A-Z0-9]{20}")
    @JsonProperty("access_key_id")
    private String accessKeyID;

    @NotNull
    @Pattern(regexp = "[a-zA-Z0-9/+]{40}")
    @JsonProperty("secret_access_key")
    private String secretAccessKey;

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

    public String getAccessKeyID() {
        return accessKeyID;
    }

    public String getSecretAccessKey() {
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
}

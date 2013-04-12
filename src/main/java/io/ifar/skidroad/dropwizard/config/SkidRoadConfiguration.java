package io.ifar.skidroad.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.db.DatabaseConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotNull;

/**
 * Configuration class for deployment within a Dropwizard service.
 */
public class SkidRoadConfiguration extends Configuration {

    @JsonProperty("node_id")
    @NotNull
    private String nodeId;

    @Valid
    @NotNull
    @JsonProperty("database")
    private DatabaseConfiguration databaseConfiguration;

    @Valid
    @NotNull
    @JsonProperty("request_log_writer")
    private RequestLogWriterConfiguration requestLogWriterConfiguration;

    @Valid
    @NotNull
    @JsonProperty("request_log_prep")
    private RequestLogPrepConfiguration requestLogPrepConfiguration;

    @Valid
    @NotNull
    @JsonProperty("request_log_upload")
    private RequestLogUploadConfiguration requestLogUploadConfiguration;

    @JsonProperty("max_quartz_threads")
    @DecimalMin(value = "1")
    private int maxQuartzThreads = Runtime.getRuntime().availableProcessors();

    public DatabaseConfiguration getDatabaseConfiguration() {
        return databaseConfiguration;
    }

    public RequestLogWriterConfiguration getRequestLogWriterConfiguration() {
        return requestLogWriterConfiguration;
    }

    public RequestLogPrepConfiguration getRequestLogPrepConfiguration() {
        return requestLogPrepConfiguration;
    }

    public RequestLogUploadConfiguration getRequestLogUploadConfiguration() {
        return requestLogUploadConfiguration;
    }

    public int getMaxQuartzThreads() {
        return maxQuartzThreads;
    }

    public String getNodeId() {
        return nodeId;
    }
}

package io.ifar.skidroad.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotNull;

/**
 *
 */
public class RequestLogWriterConfiguration {

    @JsonProperty("base_path")
    @NotEmpty
    private String basePath;

    @JsonProperty("prefix")
    @NotNull
    private String namePrefix;

    @JsonProperty("suffix")
    private String nameSuffix = ".log";

    @JsonProperty("file_flush_interval_seconds")
    @DecimalMin(value="1")
    private int fileFlushIntervalSeconds = 5;

    @JsonProperty("after_roll_close_file_delay_seconds")
    private int afterRollCloseFileDelaySeconds = 30;

    @JsonProperty("spawn_new_worker_at_queue_depth")
    @DecimalMin(value="2")
    private int spawnNewWorkerAtQueueDepth = 20;

    @JsonProperty("report_unhealthy_at_queue_depth")
    @DecimalMin(value="1")
    private int reportUnhealthyAtQueueDepth = 100;

    @JsonProperty("rolling_frequency")
    @NotNull
    private RollingFrequencyUnit rollingFrequency = RollingFrequencyUnit.hourly;

    public RequestLogWriterConfiguration() {
    }

    public RequestLogWriterConfiguration(String basePath, String namePrefix, String nameSuffix, int fileFlushIntervalSeconds, int afterRollCloseFileDelaySeconds, int spawnNewWorkerAtQueueDepth, int reportUnhealthyAtQueueDepth, RollingFrequencyUnit rollingFrequency) {
        this.basePath = basePath;
        this.namePrefix = namePrefix;
        this.nameSuffix = nameSuffix;
        this.fileFlushIntervalSeconds = fileFlushIntervalSeconds;
        this.afterRollCloseFileDelaySeconds = afterRollCloseFileDelaySeconds;
        this.spawnNewWorkerAtQueueDepth = spawnNewWorkerAtQueueDepth;
        this.reportUnhealthyAtQueueDepth = reportUnhealthyAtQueueDepth;
        this.rollingFrequency = rollingFrequency;
    }

    public RequestLogWriterConfiguration copy() {
        return new RequestLogWriterConfiguration(basePath,namePrefix,nameSuffix,fileFlushIntervalSeconds,afterRollCloseFileDelaySeconds,spawnNewWorkerAtQueueDepth,reportUnhealthyAtQueueDepth,rollingFrequency);
    }

    public int getAfterRollCloseFileDelaySeconds() {
        return afterRollCloseFileDelaySeconds;
    }

    public String getBasePath() {
        return basePath;
    }

    public int getFileFlushIntervalSeconds() {
        return fileFlushIntervalSeconds;
    }

    public String getNamePrefix() {
        return namePrefix;
    }

    public String getNameSuffix() {
        return nameSuffix;
    }

    public int getReportUnhealthyAtQueueDepth() {
        return reportUnhealthyAtQueueDepth;
    }

    public int getSpawnNewWorkerAtQueueDepth() {
        return spawnNewWorkerAtQueueDepth;
    }

    public RollingFrequencyUnit getRollingFrequency() {
        return rollingFrequency;
    }

    public RequestLogWriterConfiguration setNamePrefix(String namePrefix) {
        this.namePrefix = namePrefix;
        return this;
    }

    public RequestLogWriterConfiguration setNameSuffix(String nameSuffix) {
        this.nameSuffix = nameSuffix;
        return this;
    }
}
package io.ifar.skidroad.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

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
}
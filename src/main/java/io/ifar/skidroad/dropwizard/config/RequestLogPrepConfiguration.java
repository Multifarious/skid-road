package io.ifar.skidroad.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.DecimalMin;

/**
 *
 */
public class RequestLogPrepConfiguration {

    @JsonProperty("master_key")
    @NotEmpty
    private String masterKey;

    @JsonProperty("master_iv")
    @NotEmpty
    private String masterIV;

    @JsonProperty("report_unhealthy_at_queue_depth")
    @DecimalMin(value="1")
    private int reportUnhealthyAtQueueDepth = 10;

    public String getMasterIV() {
        return masterIV;
    }

    public String getMasterKey() {
        return masterKey;
    }

    public int getReportUnhealthyAtQueueDepth() {
        return reportUnhealthyAtQueueDepth;
    }
}
package io.ifar.skidroad.dropwizard.config;

import com.amazonaws.auth.*;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.validation.ValidationMethod;
import org.apache.commons.lang.StringUtils;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 *
 */
public class SkidRoadReadOnlyConfiguration {

    @JsonProperty("master_key")
    @NotEmpty
    private String masterKey;

    /**
     * Fixed master IV no longer used during encryption. May optionally be supplied
     * for decrypting legacy data.
     */
    @JsonProperty("master_iv")
    private String masterIV;

    @Pattern(regexp = "[A-Z0-9]{20}")
    @JsonProperty("access_key_id")
    private String accessKeyID;

    @Pattern(regexp = "[a-zA-Z0-9/+]{40}")
    @JsonProperty("secret_access_key")
    private String secretAccessKey;

    @JsonProperty("disable_certificate_checks")
    private boolean disableCertificateChecks = false;

    @JsonProperty("database")
    @Valid
    @NotNull
    private DataSourceFactory databaseConfiguration;

    public SkidRoadReadOnlyConfiguration() {
        // for Jackson and friends
    }

    protected SkidRoadReadOnlyConfiguration(String masterKey, String masterIV, String accessKeyID, String secretAccessKey, DataSourceFactory databaseConfiguration) {
        this.masterKey = masterKey;
        this.masterIV = masterIV;
        this.accessKeyID = accessKeyID;
        this.secretAccessKey = secretAccessKey;
        this.databaseConfiguration = databaseConfiguration;
    }

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

    public String getMasterKey() {
        return masterKey;
    }

    public String getMasterIV() {
        return masterIV;
    }

    public DataSourceFactory getDatabaseConfiguration() {
        return databaseConfiguration;
    }
}

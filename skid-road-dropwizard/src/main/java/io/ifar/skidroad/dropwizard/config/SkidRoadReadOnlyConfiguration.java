package io.ifar.skidroad.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.db.DatabaseConfiguration;
import com.yammer.dropwizard.validation.ValidationMethod;
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

    @JsonProperty("use_instance_profile_credentials")
    private boolean useInstanceProfileCredentials = false;

    @JsonProperty("disable_certificate_checks")
    private boolean disableCertificateChecks = false;

    @JsonProperty("database")
    @Valid
    @NotNull
    private DatabaseConfiguration databaseConfiguration;

    public SkidRoadReadOnlyConfiguration() {
        // for Jackson and friends
    }

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

    public boolean isUseInstanceProfileCredentials() {
        return useInstanceProfileCredentials;
    }

    protected SkidRoadReadOnlyConfiguration(String masterKey, String masterIV, String accessKeyID, String secretAccessKey, DatabaseConfiguration databaseConfiguration) {
        this.masterKey = masterKey;
        this.masterIV = masterIV;
        this.accessKeyID = accessKeyID;
        this.secretAccessKey = secretAccessKey;
        this.databaseConfiguration = databaseConfiguration;
    }

    public String getMasterKey() {
        return masterKey;
    }

    public String getMasterIV() {
        return masterIV;
    }

    public String getAccessKeyID() {
        return accessKeyID;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public DatabaseConfiguration getDatabaseConfiguration() {
        return databaseConfiguration;
    }
}

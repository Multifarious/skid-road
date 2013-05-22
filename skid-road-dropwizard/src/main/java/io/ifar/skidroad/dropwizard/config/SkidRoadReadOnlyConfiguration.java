package io.ifar.skidroad.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.db.DatabaseConfiguration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

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

    @JsonProperty("access_key_id")
    @NotEmpty
    private String accessKeyID;

    @JsonProperty("secret_access_key")
    @NotEmpty
    private String secretAccessKey;

    @JsonProperty("database")
    @Valid
    @NotNull
    private DatabaseConfiguration databaseConfiguration;

    public SkidRoadReadOnlyConfiguration() {
        // for Jackson and friends
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

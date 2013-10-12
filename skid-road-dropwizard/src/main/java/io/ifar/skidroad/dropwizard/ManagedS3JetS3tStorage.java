package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.dropwizard.config.RequestLogUploadConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.jets3t.S3JetS3tStorage;
import io.ifar.skidroad.upload.JetS3tUploadWorkerFactory;
import io.ifar.skidroad.upload.UploadWorkerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class ManagedS3JetS3tStorage extends S3JetS3tStorage implements Managed {
    public ManagedS3JetS3tStorage(String accessKeyID, String secretAccessKey) {
        super(accessKeyID, secretAccessKey);
    }

    public ManagedS3JetS3tStorage(String accessKeyID, String secretAccessKey, Map<String,String> propertyOverrides) {
        super(accessKeyID, secretAccessKey, propertyOverrides);
    }

    public static ManagedS3JetS3tStorage buildStorage(RequestLogUploadConfiguration uploadConfiguration, Environment environment) {
        ManagedS3JetS3tStorage storage = new ManagedS3JetS3tStorage(
                uploadConfiguration.getAccessKeyID(),
                uploadConfiguration.getSecretAccessKey()
        );
        environment.manage(storage);
        environment.addHealthCheck(storage.healthCheck());
        return storage;
    }

    public static UploadWorkerFactory buildWorkerFactory(S3JetS3tStorage storage, RequestLogUploadConfiguration uploadConfiguration) throws URISyntaxException {
        return new JetS3tUploadWorkerFactory(
                storage,
                new URI(uploadConfiguration.getUploadPath())
        );
    }

    public static UploadWorkerFactory buildWorkerFactory(RequestLogUploadConfiguration uploadConfiguration, Environment environment) throws URISyntaxException {
        return buildWorkerFactory(buildStorage(uploadConfiguration, environment), uploadConfiguration);
    }

    public static UploadWorkerFactory buildWorkerFactory(SkidRoadConfiguration skidRoadConfiguration, Environment environment) throws URISyntaxException {
        return buildWorkerFactory(skidRoadConfiguration.getRequestLogUploadConfiguration(), environment);
    }
}

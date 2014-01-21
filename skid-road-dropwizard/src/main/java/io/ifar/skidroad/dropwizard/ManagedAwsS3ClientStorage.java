package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.dropwizard.config.RequestLogUploadConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.awssdk.AwsS3ClientStorage;
import io.ifar.skidroad.upload.AwsS3ClientUploadByDirectoryWorkerFactory;
import io.ifar.skidroad.upload.AwsS3ClientUploadWorkerFactory;
import io.ifar.skidroad.upload.UploadWorkerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class ManagedAwsS3ClientStorage extends AwsS3ClientStorage implements Managed {
    public ManagedAwsS3ClientStorage(String accessKeyID, String secretAccessKey) {
        this(accessKeyID, secretAccessKey, null);
    }

    public ManagedAwsS3ClientStorage(String accessKeyID, String secretAccessKey, Map<String, String> propertyOverrides) {
        super(accessKeyID, secretAccessKey, propertyOverrides);
    }

    public static ManagedAwsS3ClientStorage buildStorage(RequestLogUploadConfiguration uploadConfiguration, Environment environment) {
        return buildStorage(uploadConfiguration, environment, null);
    }

    public static ManagedAwsS3ClientStorage buildStorage(RequestLogUploadConfiguration uploadConfiguration, Environment environment, Map<String,String> propertyOverrides) {
        ManagedAwsS3ClientStorage storage = new ManagedAwsS3ClientStorage(
                uploadConfiguration.getAccessKeyID(),
                uploadConfiguration.getSecretAccessKey(),
                propertyOverrides
        );
        environment.manage(storage);
        environment.addHealthCheck(storage.healthCheck());
        return storage;
    }

    public static UploadWorkerFactory buildWorkerFactory(AwsS3ClientStorage storage, RequestLogUploadConfiguration uploadConfiguration) throws URISyntaxException {
        return new AwsS3ClientUploadWorkerFactory(
                storage,
                new URI(uploadConfiguration.getUploadPath())
        );
    }

    public static UploadWorkerFactory buildByDirectoryWorkerFactory(AwsS3ClientStorage storage, RequestLogUploadConfiguration uploadConfiguration) throws URISyntaxException {
        return new AwsS3ClientUploadByDirectoryWorkerFactory(
                storage,
                new URI(uploadConfiguration.getUploadPath())
        );
    }

    public static UploadWorkerFactory buildWorkerFactory(RequestLogUploadConfiguration uploadConfiguration, Environment environment) throws URISyntaxException {
        return buildWorkerFactory(buildStorage(uploadConfiguration, environment), uploadConfiguration);
    }

    public static UploadWorkerFactory buildWorkerFactory(RequestLogUploadConfiguration uploadConfiguration, Environment environment, Map<String,String> propertyOverrides) throws URISyntaxException {
        return buildWorkerFactory(buildStorage(uploadConfiguration, environment, propertyOverrides), uploadConfiguration);
    }

    public static UploadWorkerFactory buildWorkerFactory(SkidRoadConfiguration skidRoadConfiguration, Environment environment) throws URISyntaxException {
        return buildWorkerFactory(skidRoadConfiguration.getRequestLogUploadConfiguration(), environment);
    }

    public static UploadWorkerFactory buildWorkerFactory(SkidRoadConfiguration skidRoadConfiguration, Environment environment,Map<String,String> propertyOverrides) throws URISyntaxException {
        return buildWorkerFactory(skidRoadConfiguration.getRequestLogUploadConfiguration(), environment, propertyOverrides);
    }


    public static UploadWorkerFactory buildByDirectoryWorkerFactory(RequestLogUploadConfiguration uploadConfiguration, Environment environment) throws URISyntaxException {
        return buildByDirectoryWorkerFactory(buildStorage(uploadConfiguration, environment), uploadConfiguration);
    }

    public static UploadWorkerFactory buildByDirectoryWorkerFactory(RequestLogUploadConfiguration uploadConfiguration, Environment environment, Map<String,String> propertyOverrides) throws URISyntaxException {
        return buildByDirectoryWorkerFactory(buildStorage(uploadConfiguration, environment, propertyOverrides), uploadConfiguration);
    }

    public static UploadWorkerFactory buildByDirectoryWorkerFactory(SkidRoadConfiguration skidRoadConfiguration, Environment environment) throws URISyntaxException {
        return buildByDirectoryWorkerFactory(skidRoadConfiguration.getRequestLogUploadConfiguration(), environment);
    }

    public static UploadWorkerFactory buildByDirectoryWorkerFactory(SkidRoadConfiguration skidRoadConfiguration, Environment environment,Map<String,String> propertyOverrides) throws URISyntaxException {
        return buildByDirectoryWorkerFactory(skidRoadConfiguration.getRequestLogUploadConfiguration(), environment, propertyOverrides);
    }
}

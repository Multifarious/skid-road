package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.jets3t.S3JetS3tStorage;

public class ManagedS3JetS3tStorage extends S3JetS3tStorage implements Managed {
    public ManagedS3JetS3tStorage(String accessKeyID, String secretAccessKey) {
        super(accessKeyID, secretAccessKey);
    }
}

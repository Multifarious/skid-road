package io.ifar.skidroad.jets3t;

import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.impl.rest.httpclient.RestStorageService;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.security.ProviderCredentials;

/**
 */
public class S3JetS3tStorage extends AbstractJetS3tStorage {
    private ProviderCredentials creds;

    public S3JetS3tStorage(String accessKeyID, String secretAccessKey) {
        super("s3","Amazon S3");
        this.creds = new AWSCredentials(accessKeyID, secretAccessKey);
    }

    @Override
    RestStorageService openStorageService() throws ServiceException {
        return new RestS3Service(creds);
    }
}

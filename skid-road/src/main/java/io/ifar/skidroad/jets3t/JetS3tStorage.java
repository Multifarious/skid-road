package io.ifar.skidroad.jets3t;

import com.yammer.metrics.core.HealthCheck;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.StorageObject;

import java.io.File;

public interface JetS3tStorage {
    HealthCheck healthCheck();

    void start() throws Exception;

    void stop() throws Exception;

    StorageObject put(String uri, File f) throws ServiceException;

    StorageObject get(String uri) throws ServiceException;
}

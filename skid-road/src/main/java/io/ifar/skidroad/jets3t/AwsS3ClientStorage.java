 package io.ifar.skidroad.jets3t;

 import com.amazonaws.AmazonClientException;
 import com.amazonaws.ClientConfiguration;
 import com.amazonaws.auth.AWSCredentials;
 import com.amazonaws.auth.BasicAWSCredentials;
 import com.amazonaws.services.s3.AmazonS3Client;
 import com.amazonaws.services.s3.model.ObjectMetadata;
 import com.amazonaws.services.s3.model.PutObjectRequest;
 import com.amazonaws.services.s3.model.S3Object;
 import com.amazonaws.services.s3.transfer.TransferManager;
 import com.amazonaws.services.s3.transfer.model.UploadResult;
 import com.google.common.base.Throwables;
 import com.yammer.metrics.core.HealthCheck;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.io.File;
 import java.io.FileInputStream;
 import java.io.IOException;
 import java.util.Collections;
 import java.util.Map;
 import java.util.concurrent.atomic.AtomicInteger;

 /**
  */
 public class AwsS3ClientStorage implements S3Storage {

     private static final Logger LOG = LoggerFactory.getLogger(AwsS3ClientStorage.class);

     private AWSCredentials creds;
     private final String name = "Amazon S3";
     private TransferManager svc;
     private final HealthCheck healthCheck;
     private final String urlScheme = "s3";
     private final Map<String,String> propertiesOverrides;

     private volatile boolean started = false;
     private volatile boolean stopping = false;

     private AtomicInteger uploadsInProgress = new AtomicInteger();
     private AtomicInteger downloadsInProgress = new AtomicInteger();

     public AwsS3ClientStorage(String accessKeyID, String secretAccessKey) {
         this(accessKeyID, secretAccessKey, null);
     }

     public AwsS3ClientStorage(String accessKeyID, String secretAccessKey, Map<String, String> propertyOverrides) {
         this.creds = new BasicAWSCredentials(accessKeyID, secretAccessKey);
         this.healthCheck = new HealthCheck(name) {
             @Override
             protected Result check() throws Exception {
                 if (!started) {
                     return Result.unhealthy("The storage service is not yet started.");
                 } else if (stopping) {
                     return Result.unhealthy("The service is stopping.");
                 } else {
                     return Result.healthy("There are %d download%s and %d upload%s in progress.",
                             downloadsInProgress.get(),downloadsInProgress.get() ==1 ? "":"s",
                             uploadsInProgress.get(),uploadsInProgress.get() ==1 ? "":"s");
                 }
             }
         };
         this.propertiesOverrides = propertyOverrides == null ? Collections.<String,String>emptyMap() : propertyOverrides;
     }

     @Override
     public HealthCheck healthCheck() {
         return healthCheck;
     }

     @Override
     public void start() throws Exception {
         LOG.info("Starting {}.", name);
         ClientConfiguration clientConfiguration = new ClientConfiguration();
         this.svc = new TransferManager(new AmazonS3Client(creds,clientConfiguration));
         LOG.info("Started {}.", name);
         started = true;
      }

     @Override
     final public void stop() throws Exception {
         stopping = true;
         LOG.info("Stopping {}.",name);
         if (svc != null) {
             svc.shutdownNow();
         }
         svc = null;
         LOG.info("Stopped {}.",name);
     }


     private String[] pieces(String uri) {
         assert uri.startsWith(urlScheme + "://") : String.format("A %s URI must start with \"%s://\".", name, urlScheme);
         String[] parts = uri.substring(urlScheme.length() + 3).split("/", 2);
         assert parts.length == 2 : String.format("A %s URI must be of the form \"%s://bucket/path\".", name, urlScheme);
         return parts;
     }

     @Override
     public void put(String uri, File f) throws AmazonClientException {
         LOG.trace("Uploading " + uri);
         String[] parts = pieces(uri);
         ObjectMetadata om = new ObjectMetadata();
         om.setContentLength(f.length());
         if (f.getName().endsWith("gzip")) {
             om.setContentEncoding("gzip");
         }
         uploadsInProgress.incrementAndGet();
         try (FileInputStream is = new FileInputStream(f)) {
             PutObjectRequest req = new PutObjectRequest(parts[0],parts[1],is,om);
             UploadResult resp = svc.upload(req).waitForUploadResult();
             LOG.trace("Uploaded " + uri + " with ETag " + resp.getETag());
         } catch (IOException ioe) {
             LOG.error("Unexpected IOException while uploading {} to {}: ({}) {}",
                     f.getPath(),uri,ioe.getClass(), ioe.getMessage());
             throw Throwables.propagate(ioe);
         } catch (InterruptedException ie) {
             LOG.error("Interrupted while uploading {} to {}.",
                     f.getPath(), uri);
             throw Throwables.propagate(ie);
         } finally {
             uploadsInProgress.decrementAndGet();
         }
     }

     @Override
     public S3Object get(String uri) throws AmazonClientException {
         String[] parts = pieces(uri);
         downloadsInProgress.incrementAndGet();
         try {
             return svc.getAmazonS3Client().getObject(parts[0],parts[1]);
         } finally {
             downloadsInProgress.decrementAndGet();
         }
     }
 }

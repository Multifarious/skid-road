 package io.ifar.skidroad.jets3t;

 import com.yammer.metrics.core.HealthCheck;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageService;
import org.jets3t.service.impl.rest.httpclient.RestStorageService;
import org.jets3t.service.model.StorageObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.Map;

 /**
  */
 public abstract class AbstractJetS3tStorage implements JetS3tStorage {

     private static final Logger LOG = LoggerFactory.getLogger(AbstractJetS3tStorage.class);

     private final String name;
     private StorageService svc;
     private final HealthCheck healthCheck;
     private final String urlScheme;
     private final Map<String,String> propertiesOverrides;

     public AbstractJetS3tStorage(final String urlScheme, final String name) {
         this(urlScheme, name, Collections.<String,String>emptyMap());
     }

     /**
      * @param propertiesOverrides used to override default JetS3t configuration
      */
     public AbstractJetS3tStorage(final String urlScheme, final String name, Map<String,String> propertiesOverrides) {
         this.urlScheme = urlScheme;
         this.name = name;
         this.healthCheck = new HealthCheck(name) {
             @Override
             protected Result check() throws Exception {
                 if (svc == null) {
                     return Result.unhealthy("No storage service is active.");
                 } else {
                     if (svc.isShutdown()) {
                         return Result.unhealthy("The storage service is shut down");
                     } else {
                         return Result.healthy();
                     }
                 }
             }
         };
         this.propertiesOverrides = propertiesOverrides;
     }

     @Override
     public HealthCheck healthCheck() {
         return healthCheck;
     }

     abstract RestStorageService openStorageService() throws ServiceException;

     @Override
     public void start() throws Exception {
         LOG.info("Starting {}.", name);
         this.svc = openStorageService();
         for (Map.Entry<String,String> override : propertiesOverrides.entrySet()) {
             svc.getJetS3tProperties().setProperty(override.getKey(), override.getValue());
         }
         LOG.info("Started {}.", name);
      }

     @Override
     final public void stop() throws Exception {
         LOG.info("Stopping {}.",name);
         if (svc != null) {
             svc.shutdown();
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
     public StorageObject put(String uri, File f) throws ServiceException {
         LOG.trace("Uploading " + uri);
         String[] parts = pieces(uri);
         StorageObject o = new StorageObject(parts[1]);
         o.setDataInputFile(f);
         o.setContentLength(f.length());
         //o.setContentType("application/json");
         if (f.getName().endsWith(".gz")) {
             o.setContentEncoding("gzip");
         }
         StorageObject result = svc.putObject(parts[0],o);
         LOG.trace("Uploaded " + uri);
         return result;
     }

     @Override
     public StorageObject get(String uri) throws ServiceException {
         String[] parts = pieces(uri);
         return svc.getObject(parts[0], parts[1]);
     }

     /*

     @Override
     public long computeSize(Collection<String> uris) throws ServiceException {
         long totalSize = 0;
         for (String gsUri : uris) {
             String[] parts = pieces(gsUri);
             StorageObject o = svc.getObjectDetails(parts[0],parts[1]);
             totalSize += o.getContentLength();
         }
         return totalSize;
     }

     @Override
     public List<String> expandGlobbedUri(String uri) throws ServiceException {
         String[] parts = pieces(uri);
         String bucket = parts[0];
         int mark = parts[1].lastIndexOf('/');
         String prefix = parts[1].substring(0,mark+1);
         String globExpression = parts[1].substring(mark+1).replaceAll("\\*",".*");
         List<String> out = new ArrayList<>();
         for (StorageObject o : svc.listObjects(bucket, prefix, "/")) {
             if (o.getKey().matches(prefix + globExpression)) {
                 out.add(String.format("%s://%s/%s", urlScheme, bucket, o.getKey()));
             }
         }
         return out;
     }

     @Override
     public List<Pair<String,Long>> listUri(String uri, String includeRegex, String excludeRegex) throws ServiceException {
         Pattern include =  includeRegex == null ? null : Pattern.compile(includeRegex);
         Pattern exclude =  excludeRegex == null ? null : Pattern.compile(excludeRegex);
         String[] parts = pieces(uri);
         String bucket = parts[0];
         String prefix = parts[1];
         StorageObject[] found = svc.listObjects(bucket, prefix, null);
         List<Pair<String,Long>> out = new ArrayList<>(found.length);
         for (StorageObject o : found)
             if (
                     (include == null || include.matcher(o.getKey()).matches()) &&
                             (exclude == null || !exclude.matcher(o.getKey()).matches()))
                 out.add(new Pair<>(String.format("%s://%s/%s", urlScheme, bucket, o.getKey()),o.getContentLength()));
         return out;
     }


     public boolean checkExistence(String uri) throws ServiceException {
         String[] parts = pieces(uri);
         return svc.isObjectInBucket(parts[0],parts[1]);
     }
     */
 }

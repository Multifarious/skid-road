package io.ifar.skidroad.dropwizard.cli;

import com.sun.jersey.core.util.Base64;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.crypto.AESInputStream;
import io.ifar.skidroad.crypto.StreamingBouncyCastleAESWithSIC;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfigurationStrategy;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileTracker;
import io.ifar.skidroad.jets3t.JetS3tStorage;
import io.ifar.skidroad.jets3t.S3JetS3tStorage;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.goodies.CliConveniences;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.commons.io.IOUtils;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.StorageObject;
import org.skife.jdbi.v2.DBI;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
/**
 * Download, decrypt, and decompress a LogFile.
 */
public abstract class FetchLogFileCommand<T extends Configuration> extends ConfiguredCommand<T>
        implements SkidRoadConfigurationStrategy<T> {
    private final static String COHORT = "cohort";
    private final static String SERIAL = "serial";
    private final static String OUT = "out";

    public FetchLogFileCommand() {
        super("fetch", "Download, decrypt, and decompress a log file.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("-c","--rolling-cohort")
                .required(true)
                .dest(COHORT)
                .help("rolling cohort of log file to fetch. E.g. '2013-03-12T03'. Note format depends on rolling scheme used to store the data.");
        subparser.addArgument("-s","--serial")
                .required(true)
                .dest(SERIAL)
                .help("serial number (within cohort) of log file to fetch. E.g. 1.").type(Integer.class);
        subparser.addArgument("-o","--out")
                .dest(OUT)
                .help("output location; defaults to current directory");
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        CliConveniences.quietLogging("io.ifar","hsqldb.db");
        LogFileTracker tracker = null;
        JetS3tStorage storage = null;
        String cohort = namespace.getString(COHORT);
        int serial = namespace.getInt(SERIAL);
        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        env.start();
        SkidRoadConfiguration skidRoadConfiguration = getSkidRoadConfiguration(configuration);
        try {

            final DBIFactory factory = new DBIFactory();
            final DBI jdbi = factory.build(env, skidRoadConfiguration.getDatabaseConfiguration(), "logfile");
            tracker = new JDBILogFileTracker(
                    new URI("http://" + skidRoadConfiguration.getNodeId()),
                    jdbi.onDemand(DefaultJDBILogFileDAO.class));
            tracker.start();

            //System.err.println(String.format("Looking up %s.%s.", cohort, serial));
            LogFile logFile = tracker.findByRollingCohortAndSerial(cohort, serial);
            if (logFile == null) {
                System.err.println(String.format("No database record for %s.%d", cohort, serial));
                return;
            }

            //System.out.println("Archive key encoded: " + logFile.getArchiveKey());
            byte[][] fileKey = StreamingBouncyCastleAESWithSIC.decodeAndDecryptKeyAndIV(
                    logFile.getArchiveKey(),
                    Base64.decode(skidRoadConfiguration.getRequestLogPrepConfiguration().getMasterKey()),
                    Base64.decode(skidRoadConfiguration.getRequestLogPrepConfiguration().getMasterIV())
            );

            if (logFile.getArchiveURI() == null) {
                System.err.println(String.format("Cannot fetch %s, no archive URI set in database.", logFile));
                return;
            }

            //System.err.println(String.format("Fetching %s", logFile.getArchiveURI()));
            storage = new S3JetS3tStorage(
                    skidRoadConfiguration.getRequestLogUploadConfiguration().getAccessKeyID(),
                    skidRoadConfiguration.getRequestLogUploadConfiguration().getSecretAccessKey()
            );
            storage.start();

            StorageObject so;
            try {
                so = storage.get(logFile.getArchiveURI().toString());
            } catch (ServiceException e) {
                System.err.println("Cannot fetch " + logFile.getArchiveURI().toString() + ": " + e.getClass().getSimpleName() + ": " + e.toString());
                return;
            }
            String outputDir = namespace.getString(OUT);
            if (outputDir == null) {
                outputDir = Paths.get(".").toAbsolutePath().getParent().toString();
            }
            Path path = Paths.get(outputDir);
            int byteCount;
            try (InputStream encryptedCompressedStream = so.getDataInputStream();
                 AESInputStream compressedStream = new AESInputStream(encryptedCompressedStream, fileKey[0], fileKey[1]);
                 GZIPInputStream plainStream = new GZIPInputStream(compressedStream)
            ) {

                if (Files.exists(path) && Files.isDirectory(path))
                    path = Paths.get(path.toString(), logFile.getOriginPath().getFileName().toString());

                OutputStream out = Files.newOutputStream( path, CREATE, WRITE);
                byteCount = IOUtils.copy(plainStream, out);
            } finally {
                so.closeDataInputStream();
            }
            System.err.println("Wrote " + byteCount + " bytes to " + path.toAbsolutePath());
        } finally {
            if (tracker != null)
                tracker.stop();
            if (storage != null)
                storage.stop();
            env.stop();
        }
    }
}

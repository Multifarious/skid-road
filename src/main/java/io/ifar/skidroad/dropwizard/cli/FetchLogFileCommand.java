package io.ifar.skidroad.dropwizard.cli;

import com.google.common.io.ByteStreams;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfigurationStrategy;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jets3t.JetS3tStorage;
import io.ifar.skidroad.jets3t.S3JetS3tStorage;
import io.ifar.skidroad.streaming.StreamingAccess;
import io.ifar.goodies.CliConveniences;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.jets3t.service.ServiceException;
import org.skife.jdbi.v2.DBI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
/**
 * Download, decrypt, and decompress a LogFile.
 */
public abstract class FetchLogFileCommand<T extends Configuration> extends ConfiguredCommand<T>
        implements SkidRoadReadOnlyConfigurationStrategy<T> {
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

        JetS3tStorage storage = null;
        String cohort = namespace.getString(COHORT);
        int serial = namespace.getInt(SERIAL);
        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        env.start();
        SkidRoadReadOnlyConfiguration skidRoadReadOnlyConfiguration = getSkidRoadReadOnlyConfiguration(configuration);
        try {

            final DBIFactory factory = new DBIFactory();
            final DBI jdbi = factory.build(env, skidRoadReadOnlyConfiguration.getDatabaseConfiguration(), "logfile");
            DefaultJDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);

            LogFile logFile = dao.findByRollingCohortAndSerial(cohort, serial);
            if (logFile == null) {
                System.err.println(String.format("No database record for %s.%d", cohort, serial));
                return;
            }

            if (logFile.getArchiveURI() == null) {
                System.err.println(String.format("Cannot fetch %s, no archive URI set in database.", logFile));
                return;
            }

            //System.err.println(String.format("Fetching %s", logFile.getArchiveURI()));
            storage = new S3JetS3tStorage(
                    skidRoadReadOnlyConfiguration.getAccessKeyID(),
                    skidRoadReadOnlyConfiguration.getSecretAccessKey()
            );
            storage.start();


            StreamingAccess access = new StreamingAccess(storage,
                    skidRoadReadOnlyConfiguration.getMasterKey(),
                    skidRoadReadOnlyConfiguration.getMasterIV()
            );

            String outputDir = namespace.getString(OUT);
            if (outputDir == null) {
                outputDir = Paths.get(".").toAbsolutePath().getParent().toString();
            }
            Path path = Paths.get(outputDir);
            long byteCount;
            try (InputStream is = access.streamFor(logFile)) {

                if (Files.exists(path) && Files.isDirectory(path))
                    path = Paths.get(path.toString(), logFile.getOriginPath().getFileName().toString());

                OutputStream out = Files.newOutputStream( path, CREATE, WRITE);
                byteCount = ByteStreams.copy(is, out);
                out.flush();
                out.close();
                System.err.println("Wrote " + byteCount + " bytes to " + path.toAbsolutePath());
            } catch (ServiceException se) {
                System.err.println(String.format("Unable to download from S3: (%d) %s",
                        se.getResponseCode(),se.getErrorMessage()));
            } catch (IOException ioe) {
                System.err.println(String.format("Unable to process stream: (%s) %s",
                        ioe.getClass().getSimpleName(), ioe.getMessage()));
            }
        } finally {
            if (storage != null) {
                storage.stop();
            }
            env.stop();
        }
    }
}

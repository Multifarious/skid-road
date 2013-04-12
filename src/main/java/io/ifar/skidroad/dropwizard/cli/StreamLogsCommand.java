package io.ifar.skidroad.dropwizard.cli;

import com.sun.jersey.core.util.Base64;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.crypto.AESInputStream;
import io.ifar.skidroad.crypto.StreamingBouncyCastleAESWithSIC;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JodaArgumentFactory;
import io.ifar.skidroad.jets3t.JetS3tStorage;
import io.ifar.skidroad.jets3t.S3JetS3tStorage;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.goodies.CliConveniences;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.commons.io.IOUtils;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.StorageObject;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.DBI;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

/**
 *
 */
public class StreamLogsCommand  extends ConfiguredCommand<SkidRoadConfiguration> {

    private final static String STATE = "state";
    private final static String START_DATE = "start";
    private final static String END_DATE = "end";
    private final static String OUT_FILE = "out";

    private final static DateTimeFormatter ISO_FMT = ISODateTimeFormat.date().withZoneUTC();

    public StreamLogsCommand() {
        super("stream-logs","List the log files stored in the system.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("-s","--state")
                .required(true)
                .dest(STATE)
                .help("");

        subparser.addArgument("-i","--start-date")
                .required(true)
                .dest(START_DATE)
                .help("a start date in ISO format (yyyy-MM-dd); only files with a start on or after this date will be included.");

        subparser.addArgument("-e","--end-date")
                .required(true)
                .dest(END_DATE)
                .help("an end date in ISO format (yyyy-MM-dd); only files with a start on or before this date will be included.");

        subparser.addArgument("-o","--out")
                .required(true)
                .dest(OUT_FILE)
                .help("the output file to write to (will be overwritten).");
    }


    @Override
    protected void run(Bootstrap<SkidRoadConfiguration> bootstrap, Namespace namespace, SkidRoadConfiguration configuration) throws Exception {
        CliConveniences.quietLogging("ifar", "hsqldb.db");
        LogFileTracker tracker = null;
        JetS3tStorage storage = null;
        String state = namespace.getString(STATE);
        DateTime startDate = ISO_FMT.parseDateTime(namespace.getString(START_DATE));
        DateTime endDate = ISO_FMT.parseDateTime(namespace.getString(END_DATE));

        String outFile = namespace.getString(OUT_FILE);

        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        env.start();
        try (FileOutputStream out = new FileOutputStream(outFile)) {
            DBIFactory factory = new DBIFactory();
            DBI jdbi = factory.build(env, configuration.getDatabaseConfiguration(), "logfile");
            jdbi.registerArgumentFactory(new JodaArgumentFactory());

            storage = new S3JetS3tStorage(
                    configuration.getRequestLogUploadConfiguration().getAccessKeyID(),
                    configuration.getRequestLogUploadConfiguration().getSecretAccessKey()
            );
            storage.start();

            JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
            Iterator<LogFile> iter = dao.listLogFilesByDateAndState(state, startDate, endDate);

            long files = dao.count(state, startDate, endDate);
            long totalBytes = dao.totalSize(state, startDate, endDate);

            System.out.print(String.format("[ %,d files / %,d total bytes ]: ",files, totalBytes));
            while(iter.hasNext()) {
                LogFile logFile = iter.next();
                if (logFile.getArchiveURI() == null) {
                    System.out.print("?");
                    System.err.print(String.format("Cannot fetch %s, no archive URI set in database.", logFile));
                    continue;
                }

                byte[][] fileKey = StreamingBouncyCastleAESWithSIC.decodeAndDecryptKeyAndIV(
                        logFile.getArchiveKey(),
                        Base64.decode(configuration.getRequestLogPrepConfiguration().getMasterKey()),
                        Base64.decode(configuration.getRequestLogPrepConfiguration().getMasterIV())
                );

                StorageObject so;
                try {
                    so = storage.get(logFile.getArchiveURI().toString());
                } catch (ServiceException e) {
                    System.out.print("X");
                    System.err.println(String.format("Cannot fetch %s due to %s from S3: (%s) %s",
                            logFile.getArchiveURI(), e.getErrorCode(), e.getClass().getSimpleName(), e.getMessage()));
                    continue;
                }

                try (InputStream encryptedCompressedStream = so.getDataInputStream();
                     AESInputStream compressedStream = new AESInputStream(encryptedCompressedStream, fileKey[0], fileKey[1]);
                     GZIPInputStream plainStream = new GZIPInputStream(compressedStream)
                ) {
                    IOUtils.copy(plainStream, out);
                } catch (IOException ioe) {
                    System.out.println("#");
                    System.err.println(String.format("Cannot process data from %s: (%s) %s",
                            logFile.getArchiveURI(), ioe.getClass().getSimpleName(), ioe.getMessage()));
                    continue;
                } finally {
                    so.closeDataInputStream();
                }
                System.out.print(".");
            }
            System.out.println("[DONE]");
        } finally {
            if (storage != null) {
                try {
                    storage.stop();
                } catch (Exception ex) {
                    // ignore
                }
            }
            env.stop();
        }
    }
}
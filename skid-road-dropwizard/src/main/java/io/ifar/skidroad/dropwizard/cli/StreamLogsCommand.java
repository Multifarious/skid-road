package io.ifar.skidroad.dropwizard.cli;

import com.amazonaws.AmazonServiceException;
import com.google.common.io.ByteStreams;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.goodies.CliConveniences;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfigurationStrategy;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAOHelper;
import io.ifar.skidroad.jdbi.JodaArgumentFactory;
import io.ifar.skidroad.awssdk.AwsS3ClientStorage;
import io.ifar.skidroad.awssdk.S3Storage;
import io.ifar.skidroad.streaming.StreamingAccess;
import io.ifar.skidroad.tracking.LogFileState;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.ResultIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
@SuppressWarnings("UnusedDeclaration")
public abstract class StreamLogsCommand <T extends Configuration> extends ConfiguredCommand<T>
        implements SkidRoadReadOnlyConfigurationStrategy<T>
{

    private final static String STATE = "state";
    private final static String START_DATE = "start";
    private final static String END_DATE = "end";
    private final static String OUT_FILE = "out";

    private final static DateTimeFormatter ISO_FMT = ISODateTimeFormat.dateOptionalTimeParser().withZoneUTC();

    public StreamLogsCommand() {
        super("stream-logs","List the log files stored in the system.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        List<String> states = new ArrayList<>();
        for (LogFileState state : LogFileState.values()) {
            states.add(state.name());
        }
        subparser.addArgument("-s","--state")
                .required(true)
                .dest(STATE)
                .choices(states)
                .nargs("+")
                .help("the state(s) of files to include");

        subparser.addArgument("-i","--start-date")
                .required(true)
                .dest(START_DATE)
                .help("a start date in ISO format (yyyy-MM-dd or yyyy-MM-ddThh:mm); only files with a start on or after this date will be included.");

        subparser.addArgument("-e","--end-date")
                .required(true)
                .dest(END_DATE)
                .help("an end date in ISO format (yyyy-MM-dd or yyyy-MM-ddThh:mm); only files with a start on or before this date will be included.");

        subparser.addArgument("-o","--out")
                .required(true)
                .dest(OUT_FILE)
                .help("the output file to write to (will be overwritten).");
    }



    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        CliConveniences.quietLogging("ifar", "hsqldb.db");
        S3Storage storage = null;
        Set<String> states = new HashSet<>(namespace.<String>getList(STATE));
        DateTime startDate = ISO_FMT.parseDateTime(namespace.getString(START_DATE));
        DateTime endDate = ISO_FMT.parseDateTime(namespace.getString(END_DATE));

        String outFile = namespace.getString(OUT_FILE);

        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        env.start();

        SkidRoadReadOnlyConfiguration skidRoadConfiguration = getSkidRoadReadOnlyConfiguration(configuration);
        try (OutputStream out = Files.newOutputStream(Paths.get(outFile))) {
            DBIFactory factory = new DBIFactory();
            DBI jdbi = factory.build(env, skidRoadConfiguration.getDatabaseConfiguration(), "logfile");
            jdbi.registerArgumentFactory(new JodaArgumentFactory());

            storage = new AwsS3ClientStorage(
                    skidRoadConfiguration.getAccessKeyID(),
                    skidRoadConfiguration.getSecretAccessKey()
            );
            storage.start();

            JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
            try (ResultIterator<LogFile> iter = JDBILogFileDAOHelper.listLogFilesByDateAndState(dao, states, startDate, endDate)) {

                long files = JDBILogFileDAOHelper.count(dao, states, startDate, endDate);
                long totalBytes = JDBILogFileDAOHelper.totalSize(dao, states, startDate, endDate);

                StreamingAccess access = new StreamingAccess(storage,
                        skidRoadConfiguration.getMasterKey(),
                        skidRoadConfiguration.getMasterIV());

                System.out.print(String.format("[ %,d files / %,d total bytes ]: ",files, totalBytes));

                while (iter.hasNext()) {
                    LogFile logFile = iter.next();
                    if (logFile.getArchiveURI() == null) {
                        System.out.print("?");
                        System.err.print(String.format("Cannot fetch %s, no archive URI set in database.", logFile));
                        continue;
                    }

                    try(InputStream is = access.streamFor(logFile)) {
                        ByteStreams.copy(is,out);
                    } catch (AmazonServiceException e) {
                        System.out.print("X");
                        System.err.println(String.format("Cannot fetch %s due to %s from S3: (%s) %s",
                                logFile.getArchiveURI(), e.getErrorCode(), e.getClass().getSimpleName(), e.getMessage()));
                        continue;
                    } catch (IOException ioe) {
                        System.out.println("#");
                        System.err.println(String.format("Cannot process data from %s: (%s) %s",
                                logFile.getArchiveURI(), ioe.getClass().getSimpleName(), ioe.getMessage()));
                        continue;
                    }
                    System.out.print(".");
                }
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

package io.ifar.skidroad.dropwizard.cli;

import com.google.common.io.ByteStreams;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.goodies.AutoCloseableIterator;
import io.ifar.goodies.CliConveniences;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfigurationStrategy;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JodaArgumentFactory;
import io.ifar.skidroad.jets3t.JetS3tStorage;
import io.ifar.skidroad.jets3t.S3JetS3tStorage;
import io.ifar.skidroad.streaming.StreamingAccess;
import io.ifar.skidroad.tracking.LogFileState;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.jets3t.service.ServiceException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.DBI;

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

    private final static DateTimeFormatter ISO_FMT = ISODateTimeFormat.date().withZoneUTC();

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
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        CliConveniences.quietLogging("ifar", "hsqldb.db");
        JetS3tStorage storage = null;
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

            storage = new S3JetS3tStorage(
                    skidRoadConfiguration.getAccessKeyID(),
                    skidRoadConfiguration.getSecretAccessKey()
            );
            storage.start();

            JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
            try (AutoCloseableIterator<LogFile> iter = new AutoCloseableIterator<LogFile>(dao.listLogFilesByDateAndState(states, startDate, endDate))) {

            long files = dao.count(states, startDate, endDate);
            long totalBytes = dao.totalSize(states, startDate, endDate);

            StreamingAccess access = new StreamingAccess(storage,
                    skidRoadConfiguration.getMasterKey(),
                    skidRoadConfiguration.getMasterIV());

            System.out.print(String.format("[ %,d files / %,d total bytes ]: ",files, totalBytes));
                for (LogFile logFile : iter) {
                if (logFile.getArchiveURI() == null) {
                    System.out.print("?");
                    System.err.print(String.format("Cannot fetch %s, no archive URI set in database.", logFile));
                    continue;
                }

                try(InputStream is = access.streamFor(logFile)) {
                    ByteStreams.copy(is,out);
                } catch (ServiceException e) {
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

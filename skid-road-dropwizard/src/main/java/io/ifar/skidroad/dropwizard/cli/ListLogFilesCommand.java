package io.ifar.skidroad.dropwizard.cli;

import com.google.common.base.Joiner;
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
import io.ifar.skidroad.tracking.LogFileState;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.ResultIterator;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
@SuppressWarnings("unused")
public abstract class ListLogFilesCommand<T extends Configuration> extends ConfiguredCommand<T>
        implements SkidRoadReadOnlyConfigurationStrategy<T>
{

    private final static String STATE = "state";
    private final static String START_DATE = "start";
    private final static String END_DATE = "end";

    private final static DateTimeFormatter ISO_FMT = ISODateTimeFormat.dateOptionalTimeParser().withZoneUTC();

    @SuppressWarnings("unused")
    public ListLogFilesCommand() {
        super("list-logs","List the log files stored in the system.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);

        subparser.addArgument("-s", "--state")
                .required(false)
                .dest(STATE)
                .help(String.format("the state(s) of files to include; use commas to separate multiple values.  Possible values are { %s }.",
                        Joiner.on(", ").join(LogFileState.values())));

        subparser.addArgument("-i","--start-date")
                .required(true)
                .dest(START_DATE)
                .help("a start date in ISO format (yyyy-MM-dd or yyyy-MM-ddThh:mm); only files with a start on or after this date will be included.");

        subparser.addArgument("-e","--end-date")
                .required(true)
                .dest(END_DATE)
                .help("an end date in ISO format (yyyy-MM-dd or yyyy-MM-ddThh:mm); only files with a start on or before this date will be included.");

    }


    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        CliConveniences.quietLogging("ifar", "hsqldb.db");
        Set<String> states = new HashSet<>();
        if (namespace.getString(STATE) != null) {
            for (String state : namespace.getString(STATE).split("\\s*,\\s*")) {
                try {
                    states.add(LogFileState.valueOf(state).name());
                } catch (IllegalArgumentException iae) {
                    System.err.println(String.format("The state \"%s\" is not one of the known states: { %s }",
                            state, Joiner.on(", ").join(LogFileState.values())));
                    System.exit(-1);
                    return;
                }
            }
        }
        DateTime startDate = ISO_FMT.parseDateTime(namespace.getString(START_DATE));
        DateTime endDate = ISO_FMT.parseDateTime(namespace.getString(END_DATE));

        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        SkidRoadReadOnlyConfiguration skidRoadConfiguration = getSkidRoadReadOnlyConfiguration(configuration);
        env.start();
        try {
            DBIFactory factory = new DBIFactory();
            DBI jdbi = factory.build(env, skidRoadConfiguration.getDatabaseConfiguration(), "logfile");
            jdbi.registerArgumentFactory(new JodaArgumentFactory());

            JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
            try (ResultIterator<LogFile> iter = (states.isEmpty())
                    ? JDBILogFileDAOHelper.listLogFilesByDate(dao,startDate,endDate)
                    : JDBILogFileDAOHelper.listLogFilesByDateAndState(dao,states, startDate, endDate)) {
                if (iter.hasNext()) {
                    System.out.println(String.format("%-14s | %-20s | %10s | %15s | %-25s","COHORT","STATE","SERIAL","SIZE","OWNER"));
                    System.out.println(StringUtils.repeat("_",14)
                            + "_|_" + StringUtils.repeat("_",20)
                            + "_|_" + StringUtils.repeat("_",10)
                            + "_|_" + StringUtils.repeat("_",15)
                            + "_|_" + StringUtils.repeat("_",25));
                }
                while (iter.hasNext()) {
                    LogFile lf = iter.next();
                    System.out.println(String.format("%-14s | %20s | %10d | %15d | %-25s",
                            lf.getRollingCohort(),
                            lf.getState(),
                            lf.getSerial(),
                            lf.getByteSize(),
                            lf.getOwnerURI()));
                }
            }
            long totalSize = (states.isEmpty())
                    ? dao.count(startDate, endDate)
                    : JDBILogFileDAOHelper.totalSize(dao,states, startDate, endDate);
            if (totalSize != 0) {
                System.out.println(StringUtils.repeat("_",14)
                        + "_|_" + StringUtils.repeat("_",20)
                        + "_|_" + StringUtils.repeat("_",10)
                        + "_|_" + StringUtils.repeat("_",15)
                        + "_|_" + StringUtils.repeat("_",25));
                System.out.println(String.format("%39s  %,15d","TOTAL SIZE",totalSize));
            }
            dao.close();
        } finally {
            env.stop();
        }
    }
}

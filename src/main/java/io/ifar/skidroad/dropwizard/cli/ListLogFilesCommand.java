package io.ifar.skidroad.dropwizard.cli;

import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JodaArgumentFactory;
import io.ifar.goodies.CliConveniences;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.DBI;

import java.util.Iterator;

/**
 *
 */
public class ListLogFilesCommand extends ConfiguredCommand<SkidRoadConfiguration> {

    private final static String STATE = "state";
    private final static String START_DATE = "start";
    private final static String END_DATE = "end";

    private final static DateTimeFormatter ISO_FMT = ISODateTimeFormat.date().withZoneUTC();

    public ListLogFilesCommand() {
        super("list-logs","List the log files stored in the system.");
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

    }


    @Override
    protected void run(Bootstrap<SkidRoadConfiguration> bootstrap, Namespace namespace, SkidRoadConfiguration configuration) throws Exception {
        CliConveniences.quietLogging("ifar", "hsqldb.db");
        String state = namespace.getString(STATE);
        DateTime startDate = ISO_FMT.parseDateTime(namespace.getString(START_DATE));
        DateTime endDate = ISO_FMT.parseDateTime(namespace.getString(END_DATE));

        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        env.start();
        try {
            DBIFactory factory = new DBIFactory();
            DBI jdbi = factory.build(env, configuration.getDatabaseConfiguration(), "logfile");
            jdbi.registerArgumentFactory(new JodaArgumentFactory());

            JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
            Iterator<LogFile> iter = dao.listLogFilesByDateAndState(state, startDate, endDate);
            if (iter.hasNext()) {
                System.out.println(String.format("%-25s | %10s | %15s | %-25s","COHORT","SERIAL","SIZE","OWNER"));
                System.out.println(StringUtils.repeat("_",25)
                        + "_|_" + StringUtils.repeat("_",10)
                        + "_|_" + StringUtils.repeat("_",15)
                        + "_|_" + StringUtils.repeat("_",25));
            }
            while (iter.hasNext()) {
                LogFile lf = iter.next();
                System.out.println(String.format("%-25s | %10d | %15d | %-25s",lf.getRollingCohort(),lf.getSerial(),lf.getByteSize(),lf.getOwnerURI()));
            }
            long totalSize = dao.totalSize(state, startDate, endDate);
            if (totalSize != 0) {
                System.out.println(StringUtils.repeat("_",25)
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

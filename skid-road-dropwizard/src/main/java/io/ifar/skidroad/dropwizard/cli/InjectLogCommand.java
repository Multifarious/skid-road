package io.ifar.skidroad.dropwizard.cli;

import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.goodies.CliConveniences;
import io.ifar.skidroad.dropwizard.ManagedWritingWorkerManager;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfigurationStrategy;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JodaArgumentFactory;
import io.ifar.skidroad.rolling.FileRollingScheme;
import io.ifar.skidroad.tracking.LogFileState;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.DBI;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;

/**
 *
 */
public abstract class InjectLogCommand<T extends Configuration> extends ConfiguredCommand<T>
        implements SkidRoadConfigurationStrategy<T>
{
    private final static String START_DATE = "start";
    private final static String FILE = "log_file";
    private final static String OWNER = "owner";

    public InjectLogCommand() {
        super("inject-log","Inject a locally stored log file into the system.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("-d","--date")
                .required(true)
                .dest(START_DATE)
                .help("a start datetime in ISO format (yyyy-MM-ddThh:mm) to allocate the log file to.");

        subparser.addArgument("-f","--file")
                .required(true)
                .dest(FILE)
                .help("path to log file to inject.");

        subparser.addArgument("-o","--owner")
                .required(false)
                .dest(OWNER)
                .help("set owner node id for injected file (overrides node_id in config)");
    }



    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        CliConveniences.quietLogging("ifar", "hsqldb.db");
        DateTime startTime = ISODateTimeFormat.dateOptionalTimeParser().withZoneUTC().parseDateTime(namespace.getString(START_DATE));
        String inFile = namespace.getString(FILE);

        String owner = namespace.getString(OWNER) == null ?
                getSkidRoadConfiguration(configuration).getNodeId() :
                namespace.getString(OWNER);

        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        env.start();

        SkidRoadConfiguration skidRoadConfiguration = getSkidRoadConfiguration(configuration);
        try {
            DBIFactory factory = new DBIFactory();
            DBI jdbi = factory.build(env, skidRoadConfiguration.getDatabaseConfiguration(), "logfile");
            jdbi.registerArgumentFactory(new JodaArgumentFactory());

            FileRollingScheme scheme = ManagedWritingWorkerManager.getFileRollingScheme(skidRoadConfiguration.getRequestLogWriterConfiguration());
            String rollingCohort = scheme.getRepresentation(startTime);

            JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);

            int serial;
            while(true) {
                serial = dao.determineNextSerial(rollingCohort) + 1;
                int rows = dao.claimIndex(rollingCohort, serial, new Timestamp(startTime.getMillis()), Paths.get(inFile).toUri().toString(), owner, new Timestamp(System.currentTimeMillis()));
                if (rows == 1) {
                    System.out.println(String.format("Created database record for %s serial %d.", rollingCohort, serial));
                    break;
                } else {
                    System.out.println(String.format("Another instance claimed %s serial %d. Will retry.", rollingCohort, serial));
                }
            }

            dao.updateSize(rollingCohort, serial, Files.size(Paths.get(inFile)), owner, new Timestamp(System.currentTimeMillis()));
            dao.updateState(rollingCohort, serial, LogFileState.WRITTEN.toString(), owner, new Timestamp(System.currentTimeMillis()));

            System.out.println("Database record marked as WRITTEN. File must remain available at provided path until it has been uploaded.");

            System.out.println("[DONE]");
        } finally {
            env.stop();
        }
    }
}

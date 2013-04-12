package io.ifar.skidroad.dropwizard.cli;

import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jets3t.JetS3tStorage;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.goodies.CliConveniences;
import net.sourceforge.argparse4j.inf.Namespace;
import org.skife.jdbi.v2.DBI;

import java.util.Iterator;

/**
 *
 */
public class ListOwnersCommand extends ConfiguredCommand<SkidRoadConfiguration> {

    public ListOwnersCommand() {
        super("list-owners","List the owning URIs for log files in the database.");
    }

    @Override
    protected void run(Bootstrap<SkidRoadConfiguration> bootstrap, Namespace namespace, SkidRoadConfiguration configuration) throws Exception {
        CliConveniences.quietLogging("ifar", "hsqldb.db");
        LogFileTracker tracker = null;
        JetS3tStorage storage = null;
        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        env.start();
        try {
            final DBIFactory factory = new DBIFactory();
            final DBI jdbi = factory.build(env, configuration.getDatabaseConfiguration(), "logfile");
            JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
            Iterator<String> ownerUris = dao.listOwnerUris();
            while (ownerUris.hasNext()) {
                System.out.println(ownerUris.next());
            }
        } finally {
            env.stop();
        }
    }
}

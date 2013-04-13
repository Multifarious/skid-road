package io.ifar.skidroad.dropwizard.cli;

import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfigurationStrategy;
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
public abstract class ListOwnersCommand <T extends Configuration> extends ConfiguredCommand<T>
        implements SkidRoadConfigurationStrategy<T> {

    public ListOwnersCommand() {
        super("list-owners","List the owning URIs for log files in the database.");
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        CliConveniences.quietLogging("ifar");
        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        env.start();
        try {
            final DBIFactory factory = new DBIFactory();
            final DBI jdbi = factory.build(env, getSkidRoadConfiguration(configuration).getDatabaseConfiguration(), "logfile");
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

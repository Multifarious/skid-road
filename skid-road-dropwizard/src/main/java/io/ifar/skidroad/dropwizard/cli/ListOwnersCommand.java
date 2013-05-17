package io.ifar.skidroad.dropwizard.cli;

import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.goodies.AutoCloseableIterator;
import io.ifar.goodies.CliConveniences;
import io.ifar.goodies.JdbiAutoCloseableIterator;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfigurationStrategy;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import net.sourceforge.argparse4j.inf.Namespace;
import org.skife.jdbi.v2.DBI;

/**
 *
 */
public abstract class ListOwnersCommand <T extends Configuration> extends ConfiguredCommand<T>
        implements SkidRoadReadOnlyConfigurationStrategy<T> {

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
            final DBI jdbi = factory.build(env, getSkidRoadReadOnlyConfiguration(configuration).getDatabaseConfiguration(), "logfile");
            JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
            try (AutoCloseableIterator<String> ownerUris = JdbiAutoCloseableIterator.wrap(dao.listOwnerUris())){
                for (String ownerUri : ownerUris)
                    System.out.println(ownerUri);
            }
        } finally {
            env.stop();
        }
    }
}

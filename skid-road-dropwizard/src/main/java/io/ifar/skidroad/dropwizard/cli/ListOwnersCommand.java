package io.ifar.skidroad.dropwizard.cli;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Environment;
import io.ifar.goodies.CliConveniences;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfigurationStrategy;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import net.sourceforge.argparse4j.inf.Namespace;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.ResultIterator;

/**
 *
 */
public abstract class ListOwnersCommand <T extends Configuration> extends EnvironmentCommand<T>
        implements SkidRoadReadOnlyConfigurationStrategy<T> {

    public ListOwnersCommand(Application<T> application) {
        super(application,"list-owners","List the owning URIs for log files in the database.");
    }

    @Override
    protected void run(Environment env, Namespace namespace, T configuration) throws Exception {
        CliConveniences.quietLogging("ifar");

        final DBIFactory factory = new DBIFactory();
        final DBI jdbi = factory.build(env, getSkidRoadReadOnlyConfiguration(configuration).getDatabaseConfiguration(), "logfile");
        JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
        try (ResultIterator<String> ownerUris = dao.listOwnerUris()) {
            while (ownerUris.hasNext()) {
                System.out.println(ownerUris.next());
            }
        }

    }
}

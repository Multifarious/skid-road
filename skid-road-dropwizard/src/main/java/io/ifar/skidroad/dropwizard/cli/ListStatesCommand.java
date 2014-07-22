package io.ifar.skidroad.dropwizard.cli;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Environment;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfigurationStrategy;
import io.ifar.goodies.CliConveniences;
import io.ifar.skidroad.jdbi.CountByState;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang.StringUtils;
import org.skife.jdbi.v2.DBI;

import java.util.List;

/**
 *
 */
public abstract class ListStatesCommand<T extends Configuration> extends EnvironmentCommand<T>
        implements SkidRoadReadOnlyConfigurationStrategy<T> {

    public ListStatesCommand(Application<T> application) {
        super(application,"list-states","List the file states currently in the database with counts.");
    }

    @Override
    protected void run(Environment env, Namespace namespace, T configuration) throws Exception {
        CliConveniences.quietLogging("ifar", "hsqldb.db");

        final DBIFactory factory = new DBIFactory();
        final DBI jdbi = factory.build(env, getSkidRoadReadOnlyConfiguration(configuration).getDatabaseConfiguration(), "logfile");
        JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
        List<CountByState> counts = dao.countLogFilesByState();
        if (!counts.isEmpty()) {
            System.out.println(String.format("%-20s | %-10s", "STATE", "COUNT"));
            System.out.println(String.format("%s_|_%s", StringUtils.repeat("_", 20), StringUtils.repeat("_", 10)));
        }
        for (CountByState cbs : counts) {
            System.out.println(String.format("%-20s | %-10d", cbs.getState(), cbs.getCount()));
        }
    }
}
